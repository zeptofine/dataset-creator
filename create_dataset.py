from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from multiprocessing import Pool, cpu_count, freeze_support
from pathlib import Path

import cv2
import numpy as np
import typer
from typer import Option
from typing_extensions import Annotated

import src.datarules.data_rules as drules
import src.datarules.image_rules as irules
import src.image_filters
from src.configs import FilterData, Input, MainConfig, Output
from src.datarules.base_rules import File, Filter, Producer, Rule

CPU_COUNT = int(cpu_count())
app = typer.Typer()


@dataclass
class OutputScenario:
    path: str
    filters: dict[Callable, FilterData]


@dataclass
class FileScenario:
    file: File
    outputs: list[OutputScenario]


def read_image(path: str) -> np.ndarray:
    return cv2.imread(path, cv2.IMREAD_UNCHANGED)


def parse_scenario(sc: FileScenario):
    img: np.ndarray
    original: np.ndarray

    original = read_image(str(sc.file.absolute_pth))
    mtime: os.stat_result = os.stat(str(sc.file.absolute_pth))
    for output in sc.outputs:
        img = original
        for filter_, args in output.filters.items():
            img = filter_(img, **args)

        Path(output.path).parent.mkdir(parents=True, exist_ok=True)

        cv2.imwrite(output.path, img)
        os.utime(output.path, (mtime.st_atime, mtime.st_mtime))


@app.command()
def main(
    config_path: Annotated[Path, Option(help="Where the dataset config is placed")] = Path("config.json"),
    database_path: Annotated[Path, Option(help="Where the database is placed")] = Path("filedb.arrow"),
    threads: Annotated[int, Option(help="multiprocessing threads")] = CPU_COUNT * 3 // 4,
    chunksize: Annotated[int, Option(help="imap chunksize")] = 5,
    pchunksize: Annotated[int, Option("-p", help="chunksize when populating the df")] = 100,
    pinterval: Annotated[int, Option("-s", help="save interval in secs when populating the df")] = 60,
    simulate: Annotated[bool, Option(help="stops before conversion")] = False,
    verbose: Annotated[bool, Option(help="prints converted files")] = False,
    sort_by: Annotated[str, Option(help="Which database column to sort by")] = "path",
) -> int:
    """Takes a crap ton of images and creates dataset pairs"""

    from datetime import datetime

    import rich.progress as progress
    import ujson
    from polars import DataFrame, concat
    from rich.console import Console
    from rich.progress import Progress

    from src.datarules.dataset_builder import DatasetBuilder, chunk_split
    from src.file_list import get_file_list

    c = Console(record=True)
    with Progress(
        progress.TaskProgressColumn(),
        progress.BarColumn(bar_width=20),
        progress.TimeRemainingColumn(),
        progress.TextColumn("[progress.description]{task.description}:"),
        progress.MofNCompleteColumn(),
        progress.SpinnerColumn(),
        console=c,
    ) as p:
        if not config_path.exists():
            p.log(f"{config_path} does not exist. create it in the gui and restart this program.")
            return 0

        with config_path.open("r") as f:
            cfg: MainConfig = ujson.load(f)

        db = DatasetBuilder(db_path=Path(database_path))

        def check_for_images(lst: list) -> bool:
            if not lst:
                p.log("No images found in image list")
                return False
            return True

        # generate `Input`s
        inputs: list[Input] = [
            Input(
                Path(folder["data"]["folder"]),
                folder["data"]["expressions"],
            )
            for folder in cfg["inputs"]
        ]

        # generate `Output`s
        outputs: list[Output] = [
            Output(
                Path(folder["data"]["folder"]),
                {Filter.all_filters[filter_["name"]]: filter_["data"] for filter_ in folder["data"]["lst"]},
                folder["data"]["output_format"],
            )
            for folder in cfg["output"]
        ]
        for output in outputs:
            output.path.mkdir(parents=True, exist_ok=True)

        # generate `Producer`s
        producers: list[Producer] = [Producer.all_producers[p["name"]].from_cfg(p["data"]) for p in cfg["producers"]]

        # generate `Rule`s
        rules: list[Rule] = [Rule.all_rules[r["name"]].from_cfg(r["data"]) for r in cfg["rules"]]

        db.add_rules(*rules)
        db.add_producers(*producers)

        if verbose:
            p.log(
                "inputs:",
                inputs,
                "producers:",
                producers,
                "rules:",
                rules,
                "outputs:",
                outputs,
                db,
            )

        # Gather images
        images: dict[Path, list[Path]] = {}
        count_t = p.add_task("Gathering", total=None)
        folder_t = p.add_task("from folder", total=len(inputs))
        for folder in inputs:
            lst: list[Path] = []
            for file in get_file_list(folder.path, *folder.expressions):
                lst.append(file)
                p.advance(count_t)
            images[folder.path] = lst
            p.advance(folder_t)
        p.remove_task(folder_t)
        resolved: dict[str, File] = {
            str((src / pth).resolve()): File(
                str(pth),
                str(src),
                str(pth.relative_to(src).parent),
                pth.stem,
                pth.suffix[pth.suffix[0] == "." :],
            )
            for src, lst in images.items()
            for pth in lst
        }
        diff: int = sum(map(len, images.values())) - len(resolved)
        if diff:
            p.log(f"removed {diff} conflicting symlinks")
        total_images = len(resolved)
        p.update(count_t, total=total_images, completed=total_images)

        db.add_new_paths(set(resolved))
        db_schema = db.type_schema
        db.comply_to_schema(db_schema)
        unfinished: DataFrame = db.get_unfinished()
        total_t = p.add_task("populating df", total=None)
        if not unfinished.is_empty():
            if finished := db.remove_unfinished_producers():
                p.log(f"Skipping finished producers: {finished}")

            def trigger_save(save_timer: datetime, collected: list[DataFrame]) -> tuple[datetime, list[DataFrame]]:
                if ((new_time := datetime.now()) - save_timer).total_seconds() > pinterval:
                    data: DataFrame = concat(collected, how="diagonal")
                    db.update(data)
                    db.save_df()
                    if verbose:
                        p.log(f"saved at {new_time}")
                    return new_time, []
                return save_timer, collected

            collected: list[DataFrame] = []
            save_timer: datetime = datetime.now()
            chunk: DataFrame
            p.update(total_t, total=len(unfinished))
            null_t = p.add_task("nulls set", total=None)
            chunk_t = p.add_task("chunk")
            cnt = 0
            for schemas, df in db.split_files_via_nulls(unfinished):
                if verbose:
                    p.log(df)
                    p.log(schemas)
                chunks = list(chunk_split(df, chunksize=pchunksize))
                p.update(chunk_t, total=len(chunks), completed=0)

                for (_, size), chunk in chunks:
                    for schema in schemas:
                        chunk = chunk.with_columns(**schema)
                    chunk = chunk.select(db_schema)
                    collected.append(chunk)
                    save_timer, collected = trigger_save(save_timer, collected)

                    p.advance(chunk_t)
                    p.advance(total_t, size)

                cnt += 1
                p.advance(null_t)
            p.update(null_t, total=cnt, completed=cnt)

            concatted: DataFrame = concat(collected, how="diagonal")
            # This breaks with datatypes like Array(3, pl.UInt32). Not sure why.
            # `pyo3_runtime.PanicException: implementation error, cannot get ref Array(Null, 0) from Array(UInt32, 3)`
            db.update(concatted)
            db.save_df()

            p.remove_task(chunk_t)
            p.remove_task(null_t)
            p.update(total_t, completed=len(unfinished), total=len(unfinished))
        else:
            p.remove_task(total_t)
        if verbose:
            p.log(db.df)

        filter_t = p.add_task("filtering", total=1)
        files: list[File] = [resolved[file] for file in db.filter(set(resolved))]
        p.update(filter_t, total=len(files), completed=len(files))

        if not check_for_images(files):
            return 0
        if simulate:
            p.log(f"Simulated. {len(files)} images remain.")
            return 0

        # Generate FileScenarios
        scenarios: list[FileScenario] = [
            FileScenario(
                file,
                [
                    OutputScenario(
                        str(output.path / Path(output.format_file(file))),
                        output.filters,
                    )
                    for output in outputs
                ],
            )
            for file in p.track(files, description="generating scenarios")
        ]
        # # * convert files. Finally!
        try:
            with Pool(threads) as pool:
                pool_t = p.add_task("parsing scenarios", total=len(scenarios))
                for file in pool.imap(parse_scenario, scenarios, chunksize=chunksize):
                    if verbose:
                        p.log(f"finished: {file}")
                    p.advance(pool_t)
        except KeyboardInterrupt:
            print(-1, "KeyboardInterrupt")
            return 1

        return 0


if __name__ == "__main__":
    freeze_support()
    app()
