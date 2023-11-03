import json
import logging
from datetime import datetime
from multiprocessing import Pool, cpu_count, freeze_support
from pathlib import Path
from pprint import pformat

import rich.progress as progress
import typer
from polars import DataFrame, concat
from rich import print as rprint
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, TaskID
from typer import Option
from typing_extensions import Annotated

from . import (
    ConfigHandler,
    DatasetBuilder,
    File,
    FileScenario,
    MainConfig,
    chunk_split,
)

CPU_COUNT = int(cpu_count())
logging.basicConfig(level=logging.INFO, format="%(message)s", datefmt="[%X]", handlers=[RichHandler()])
app = typer.Typer(pretty_exceptions_show_locals=True, pretty_exceptions_short=True)
log = logging.getLogger()


@app.command()
def main(
    config_path: Annotated[Path, Option(help="Where the dataset config is placed")] = Path(
        "config.json"
    ),
    database_path: Annotated[Path, Option(help="Where the database is placed")] = Path("filedb.arrow"),
    threads: Annotated[int, Option(help="multiprocessing threads")] = CPU_COUNT * 3 // 4,
    chunksize: Annotated[int, Option(help="imap chunksize")] = 5,
    population_chunksize: Annotated[int, Option(help="chunksize when populating the df")] = 100,
    population_interval: Annotated[int, Option(help="save interval in secs when populating the df")] = 60,
    simulate: Annotated[bool, Option(help="stops before conversion")] = False,
    verbose: Annotated[bool, Option(help="prints converted files")] = False,
    sort_by: Annotated[str, Option(help="Which database column to sort by")] = "path",
) -> int:
    """Takes a crap ton of images and creates dataset pairs"""
    if not config_path.exists():
        rprint(
            f"{config_path} does not exist."
            " create it in the gui ([italic]imdataset-creator-gui[/italic]"
            " or [italic]python -m imdataset-creator.gui[/italic]) and restart this program."
        )
        return 0

    with config_path.open("r") as f:
        cfg: MainConfig = json.load(f)

    c = Console(record=True)
    with Progress(
        progress.TaskProgressColumn(),
        progress.BarColumn(bar_width=20),
        progress.TimeRemainingColumn(),
        progress.TextColumn("[progress.description]{task.description}"),
        progress.MofNCompleteColumn(),
        progress.SpinnerColumn(),
        console=c,
    ) as p, DatasetBuilder(db_path=Path(database_path)) as db:
        db_cfg = ConfigHandler(cfg)
        db.add_rules(*db_cfg.rules)
        db.add_producers(*db_cfg.producers)

        count_t: TaskID
        total_t: TaskID
        null_t: TaskID
        chunk_t: TaskID

        if verbose:
            p.log(pformat(db_cfg))
        # Gather images
        resolved: dict[str, File] = {}
        count_t = p.add_task("Gathering", total=None)

        for folder, lst in db_cfg.gather_images(sort=True, reverse=True):
            for pth in lst:
                resolved[str((folder / pth).resolve())] = File.from_src(folder, pth)
            p.update(count_t, advance=len(lst))

        if diff := p.tasks[count_t].completed - len(resolved):
            p.log(f"removed an estimated {diff} conflicting symlinks")

        p.update(count_t, total=len(resolved), completed=len(resolved))

        total_t = p.add_task("populating df", total=None)
        db.add_new_paths(set(resolved))
        db.comply_to_schema(db.type_schema, in_place=True)

        unfinished: DataFrame = db.get_unfinished_existing().collect()
        if not unfinished.is_empty():
            p.update(total_t, total=len(unfinished))
            null_t = p.add_task("nulls set", total=None)
            chunk_t = p.add_task("chunk")
            if finished := db.remove_finished_producers():
                p.log(f"Skipping finished producers: {finished}")

            collected: list[DataFrame] = []
            old_collected: list[DataFrame]
            save_timer: datetime = datetime.now()
            chunk: DataFrame
            cnt = 0
            for schemas, df in db.split_files_via_nulls(unfinished):
                if verbose:
                    p.log(df)
                    p.log(schemas)
                chunks = list(chunk_split(df, chunksize=population_chunksize))
                p.update(chunk_t, total=len(chunks), completed=0)
                for chunk in db.populate_chunks(chunks, schemas):
                    collected.append(chunk)
                    if verbose:
                        print(chunk)
                    old_collected = collected
                    save_timer, collected = db.trigger_save_via_time(
                        save_timer, collected, interval=population_interval
                    )
                    if old_collected is not collected:
                        p.log(f"Saved at {save_timer}")

                    p.advance(chunk_t)
                    p.advance(total_t, len(chunk))

                cnt += 1
                p.advance(null_t)
            p.update(null_t, total=cnt, completed=cnt)
            if collected:
                concatenated: DataFrame = concat(collected, how="diagonal")
                # This breaks with datatypes like Array(3, pl.UInt32). Not sure why.
                # `pyo3_runtime.PanicException: implementation error, cannot get ref Array(Null, 0) from Array(UInt32, 3)`
                db.update(concatenated)
                db.save_df()

            p.remove_task(chunk_t)
            p.remove_task(null_t)
            p.update(total_t, completed=len(unfinished), total=len(unfinished))
        else:
            p.remove_task(total_t)
        if verbose:
            p.log(db.df)

        files: list[File]
        if db_cfg.rules:
            filter_t = p.add_task("filtering", total=0)
            files = [resolved[file] for file in db.filter(set(resolved)).get_column("path")]
            p.update(filter_t, total=len(files), completed=len(files))
        else:
            files = list(resolved.values())

        scenarios = list(db_cfg.parse_files(p.track(files, description="parsing files")))
        if len(scenarios) != len(files):
            p.log(f"{len(files) - len(scenarios)} files are completed")

        if not scenarios:
            p.log("Finished. No images remain")
            return 0
        if simulate:
            p.log(f"Simulated. {len(scenarios)} images remain")
            return 0

        try:
            start_t = datetime.now()
            execute_t = p.add_task("executing scenarios", total=len(scenarios))
            with Pool(min(threads, len(scenarios))) as pool:
                for file in pool.imap(FileScenario.run, scenarios, chunksize=chunksize):
                    if verbose:
                        p.log(f"finished: {file}")
                    p.advance(execute_t)
            p.log(f"Finished in {datetime.now() - start_t}")
        except KeyboardInterrupt:
            print(-1, "KeyboardInterrupt")
            return 1
        return 0


if __name__ == "__main__":
    freeze_support()
    app()
