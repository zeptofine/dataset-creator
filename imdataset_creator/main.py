# from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Pool, cpu_count, freeze_support
from pathlib import Path
from typing import Generator

import cv2
import numpy as np
import rich.progress as progress
import typer
from polars import DataFrame, concat
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress
from typer import Option
from typing_extensions import Annotated

from . import (
    ConfigHandler,
    DatasetBuilder,
    File,
    FileScenario,
    Input,
    MainConfig,
    Output,
    OutputScenario,
    Producer,
    Rule,
    chunk_split,
)

CPU_COUNT = int(cpu_count())
logging.basicConfig(level=logging.INFO, format="%(message)s", datefmt="[%X]", handlers=[RichHandler()])
app = typer.Typer(pretty_exceptions_show_locals=True, pretty_exceptions_short=True)
log = logging.getLogger()


def read_image(path: str) -> np.ndarray:
    return cv2.imread(path, cv2.IMREAD_UNCHANGED)


def get_outputs(file, outputs: Iterable[Output]):
    return [
        OutputScenario(str(pth), output.filters)
        for output in outputs
        if not (pth := output.folder / Path(output.format_file(file))).exists() or output.overwrite
    ]


def parse_files(files: Iterable[File], outputs: list[Output]) -> Generator[FileScenario, None, None]:
    for file in files:
        if out_s := get_outputs(file, outputs):
            yield FileScenario(file, out_s)


def gather_images(inputs: Iterable[Input]) -> Generator[tuple[Path, list[Path]], None, None]:
    for input_ in inputs:
        yield input_.folder, list(input_.run())


@app.command()
def main(
    config_path: Annotated[Path, Option(help="Where the dataset config is placed")] = Path("config.json"),
    database_path: Annotated[Path, Option(help="Where the database is placed")] = Path("filedb.arrow"),
    threads: Annotated[int, Option(help="multiprocessing threads")] = CPU_COUNT * 3 // 4,
    chunksize: Annotated[int, Option(help="imap chunksize")] = 5,
    population_chunksize: Annotated[int, Option("-p", help="chunksize when populating the df")] = 100,
    population_interval: Annotated[int, Option("-s", help="save interval in secs when populating the df")] = 60,
    simulate: Annotated[bool, Option(help="stops before conversion")] = False,
    verbose: Annotated[bool, Option(help="prints converted files")] = False,
    sort_by: Annotated[str, Option(help="Which database column to sort by")] = "path",
) -> int:
    """Takes a crap ton of images and creates dataset pairs"""
    if not config_path.exists():
        log.error(
            f"{config_path} does not exist. create it in the gui (imdataset-creator-gui or gui.py) and restart this program."
        )
        return 0

    with config_path.open("r") as f:
        cfg: MainConfig = json.load(f)

    db = DatasetBuilder(db_path=Path(database_path))

    db_cfg = ConfigHandler(cfg)
    inputs: list[Input] = db_cfg.inputs
    outputs: list[Output] = db_cfg.outputs
    producers: list[Producer] = db_cfg.producers
    rules: list[Rule] = db_cfg.rules
    db.add_rules(*rules)
    db.add_producers(*producers)

    c = Console(record=True)
    with Progress(
        progress.TaskProgressColumn(),
        progress.BarColumn(bar_width=20),
        progress.TimeRemainingColumn(),
        progress.TextColumn("[progress.description]{task.description}"),
        progress.MofNCompleteColumn(),
        progress.SpinnerColumn(),
        console=c,
    ) as p:
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
        for folder, lst in gather_images(inputs):
            images[folder] = lst
            p.update(count_t, advance=len(lst))

        resolved: dict[str, File] = {
            str((src / pth).resolve()): File.from_src(src, pth) for src, lst in images.items() for pth in lst
        }
        if diff := sum(map(len, images.values())) - len(resolved):
            p.log(f"removed an estimated {diff} conflicting symlinks")

        p.update(count_t, total=len(resolved), completed=len(resolved))

        total_t = p.add_task("populating df", total=None)
        db.add_new_paths(set(resolved))
        db_schema = db.type_schema
        db.comply_to_schema(db_schema)
        unfinished: DataFrame = db.get_unfinished()
        if not unfinished.is_empty():
            if finished := db.remove_unfinished_producers():
                p.log(f"Skipping finished producers: {finished}")

            def trigger_save(save_timer: datetime, collected: list[DataFrame]) -> tuple[datetime, list[DataFrame]]:
                if ((new_time := datetime.now()) - save_timer).total_seconds() > population_interval:
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
                chunks = list(chunk_split(df, chunksize=population_chunksize))
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

        scenarios = list(parse_files(p.track(files, description="parsing scenarios"), outputs))

        if not scenarios:
            p.log("Finished. No images remain.")
            return 0
        if simulate:
            p.log(f"Simulated. {len(scenarios)} images remain.")
            return 0

        try:
            with Pool(threads) as pool:
                execute_t = p.add_task("executing scenarios", total=len(scenarios))
                for file in pool.imap(FileScenario.run, scenarios, chunksize=chunksize):
                    if verbose:
                        p.log(f"finished: {file}")
                    p.advance(execute_t)
        except KeyboardInterrupt:
            print(-1, "KeyboardInterrupt")
            return 1
        return 0


if __name__ == "__main__":
    freeze_support()
    app()
