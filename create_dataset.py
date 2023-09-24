import os
from dataclasses import dataclass
from multiprocessing import Pool, cpu_count, freeze_support
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import cv2
import typer
from cfg_param_wrapper import CfgDict, wrap_config
from typer import Option
from typing_extensions import Annotated

from src.datarules.base_rules import Rule

if TYPE_CHECKING:
    from collections.abc import Generator

    import numpy as np

CPU_COUNT = int(cpu_count())
app = typer.Typer()


@dataclass
class Scenario:
    """A scenario which fileparse parses and creates hr/lr pairs with."""

    path: Path
    hr_path: Path
    lr_path: Path | None
    scale: int


def fileparse(dfile: Scenario) -> Scenario:
    """Converts an image file to HR and LR versions and saves them to the specified folders.

    Parameters
    ----------
    dfile : Scenario
        The Scenario object to get info from and generate the files from

    Returns
    -------
    Scenario
        Just passes the dfile back.
    """
    # Read the image file
    image: np.ndarray[int, np.dtype[np.generic]] = cv2.imread(str(dfile.path), cv2.IMREAD_UNCHANGED)
    scale = float(dfile.scale)
    image = image[0 : int((image.shape[0] // scale) * scale), 0 : int((image.shape[1] // scale) * scale)]
    mtime: float = dfile.path.stat().st_mtime
    # Save the HR / LR version of the image
    # TODO: Create a dynamic input / output system so this could be replaced with a list ofoutputs with actions

    if not dfile.hr_path.exists():
        cv2.imwrite(str(dfile.hr_path), image)
        os.utime(str(dfile.hr_path), (mtime, mtime))

    if dfile.lr_path is not None and not dfile.lr_path.exists():
        cv2.imwrite(str(dfile.lr_path), cv2.resize(image, (int(image.shape[1] // scale), int(image.shape[0] // scale))))
        os.utime(str(dfile.lr_path), (mtime, mtime))

    return dfile


config = CfgDict("config.toml", save_mode="toml", autofill=True)


@app.command()
@wrap_config(config)
def main(
    input_folder: Annotated[Path, Option(help="the folder to scan")] = Path(),
    scale: Annotated[int, Option(help="the scale to downscale")] = 4,
    extension: Annotated[Optional[str], Option(help="export extension")] = None,
    extensions: Annotated[str, Option(help="extensions to search for (split with commas)")] = "webp,png,jpg",
    config_path: Annotated[Path, Option(help="Where the rule config is placed")] = Path("database_config.toml"),
    recursive: Annotated[bool, Option(help="preserves the tree hierarchy", rich_help_panel="modifiers")] = False,
    underscores: Annotated[bool, Option(help="replaces spaces with underscores", rich_help_panel="modifiers")] = False,
    threads: Annotated[int, Option(help="multiprocessing threads", rich_help_panel="modifiers")] = CPU_COUNT * 3 // 4,
    chunksize: Annotated[int, Option(help="imap chunksize", rich_help_panel="modifiers")] = 5,
    simulate: Annotated[bool, Option(help="stops before conversion", rich_help_panel="modifiers")] = False,
    purge: Annotated[bool, Option(help="deletes output before conversion", rich_help_panel="modifiers")] = False,
    purge_all: Annotated[bool, Option(help="deletes *everything* in output", rich_help_panel="modifiers")] = False,
    make_lr: Annotated[bool, Option(help="whether to make an LR folder", rich_help_panel="modifiers")] = True,
    overwrite: Annotated[bool, Option(help="overwrites existing files", rich_help_panel="modifiers")] = False,
    verbose: Annotated[bool, Option(help="prints converted files", rich_help_panel="modifiers")] = False,
    sort_by: Annotated[str, Option(help="Which database column to sort by", rich_help_panel="modifiers")] = "path",
    use_stat: Annotated[
        bool,
        Option(
            "-s",
            help="use statrule (uses stat information to check age of a file)",
            rich_help_panel="rules",
        ),
    ] = False,
    use_res: Annotated[
        bool,
        Option(
            "-r",
            help="use resrule (specify resolution controls)",
            rich_help_panel="rules",
        ),
    ] = False,
    use_hash: Annotated[
        bool,
        Option(
            "-h",
            help="use hashrule (use image hashes to remove visually similar images)",
            rich_help_panel="rules",
        ),
    ] = False,
    use_channel: Annotated[
        bool,
        Option(
            "-c",
            help="use channelrule (checks number of channels in an image)",
            rich_help_panel="rules",
        ),
    ] = False,
    use_blw: Annotated[
        bool,
        Option(
            "-b",
            help="use blacknwhitelistrule (checks for strings in a file path)",
            rich_help_panel="rules",
        ),
    ] = False,
    use_lim: Annotated[bool, Option("-l", help="limit total num of files", rich_help_panel="rules")] = False,
) -> int:
    """Takes a crap ton of images and creates HR / LR pairs"""

    from itertools import chain

    import rich.progress as progress
    from polars import DataFrame, concat
    from rich.console import Console
    from rich.progress import Progress

    import src.datarules.data_rules as drules
    import src.datarules.image_rules as irules
    from src.datarules.custom_toml import TomlCustomCommentDecoder, TomlCustomCommentEncoder
    from src.datarules.dataset_builder import DatasetBuilder, chunk_split
    from util.file_list import get_file_list, to_recursive

    c = Console(record=True)
    with Progress(
        progress.TaskProgressColumn(),
        progress.BarColumn(bar_width=20),
        progress.TimeRemainingColumn(),
        progress.TextColumn("[progress.description]{task.description}:"),
        progress.MofNCompleteColumn(),
        progress.SpinnerColumn(spinner_name="material"),
        console=c,
        # expand=True,
        # transient=True,
    ) as p:
        cfg = CfgDict(
            config_path,
            {"save_interval": 60, "chunksize": 100, "filepath": "filedb.feather"},
            autofill=False,
            save_on_change=False,
            start_empty=True,
            save_mode="toml",
            encoder=TomlCustomCommentEncoder(),
            decoder=TomlCustomCommentDecoder(),
        )
        db = DatasetBuilder(db_path=Path(cfg["filepath"]))
        if not config_path.exists():
            db.add_rules(
                drules.StatRule,
                irules.ResRule,
                irules.HashRule,
                irules.ChannelRule,
                drules.BlacknWhitelistRule,
                drules.TotalLimitRule,
            )
            cfg.update(db.generate_config()).save()
            p.log(f"{config_path} created. edit it and restart this program.")
            return 0
        cfg.load()

        def check_for_images(image_list: list[Path]) -> bool:
            if not image_list:
                p.log(f"No images found in {input_folder}")
                return False
            return True

        def recurse(path: Path | str):
            return to_recursive(path, recursive, underscores)

        if not input_folder:
            print("Please select a directory.")
            return 1
        if not input_folder.exists():
            print(f"Folder {input_folder} does not exist.")
            return 1

        if extension:
            if extension.startswith("."):
                extension = extension[1:]
            if extension.lower() in ["self", "none", "same", ""]:
                extension = None

        # * get hr / lr folders
        hr_folder: Path = input_folder.parent / f"{scale}xHR"
        lr_folder: Path = input_folder.parent / f"{scale}xLR"
        if extension:
            hr_folder = hr_folder.with_name(f"{hr_folder.name}-{extension}")
            lr_folder = lr_folder.with_name(f"{lr_folder.name}-{extension}")
        hr_folder.mkdir(parents=True, exist_ok=True)
        if make_lr:
            lr_folder.mkdir(parents=True, exist_ok=True)

        def hrlr_pair(path: Path) -> tuple[Path, Path | None]:
            """Gets the HR and LR file paths for a given file or dir path.

            Parameters
            ----------
            path : Path
                the path to use.

            Returns
            -------
            tuple[Path, Path]
                HR and LR file paths.
            """
            # Create the HR and LR folders if they do not exist
            hr_path: Path = hr_folder / recurse(path)
            hr_path.parent.mkdir(parents=True, exist_ok=True)
            if extension:
                hr_path = hr_path.with_suffix(f".{extension}")
            lr_path: Path | None = None
            if make_lr:
                lr_path = lr_folder / recurse(path)
                lr_path.parent.mkdir(parents=True, exist_ok=True)
                if extension:
                    lr_path = lr_path.with_suffix(f".{extension}")

            return hr_path, lr_path

        # * Gather images
        g_t = p.add_task("gathering images", total=None)

        available_extensions: list[str] = extensions.split(",")
        p.log(f"Searching extensions: {available_extensions}")

        file_list: Generator[Path, None, None] = get_file_list(
            input_folder, *(f"*.{ext}" for ext in available_extensions)
        )
        image_list: list[Path] = [x.relative_to(input_folder) for x in file_list]
        p.update(g_t, advance=len(image_list), total=len(image_list))

        rules: list[type[Rule] | Rule] = []
        producers = set()
        if use_stat:
            rules.append(drules.StatRule)
            producers.add(drules.FileInfoProducer())
        if use_res or use_channel:
            producers.add(irules.ImShapeProducer())
        if use_channel:
            rules.append(irules.ChannelRule)
        if use_res:
            rules.append(irules.ResRule)
        if use_hash:
            rules.append(irules.HashRule(cfg["hashing"]["resolver"]))
            producers.add(irules.HashProducer(cfg["hashing"]["hasher"]))
        if use_blw:
            rules.append(drules.BlacknWhitelistRule)
        if use_lim:
            rules.append(drules.TotalLimitRule)
        db.add_rules(*rules)
        db.add_producers(producers)
        db.fill_from_config(cfg)

        # * Purge existing images
        if purge_all or purge:
            p_t = p.add_task("purging")
            if purge_all:
                # This could be cleaner
                to_delete = set(chain(get_file_list(hr_folder, "*"), get_file_list(lr_folder, "*")))
                if to_delete:
                    for file in p.track(to_delete, total=len(to_delete), task_id=p_t, description="Purging"):
                        if file.is_file():
                            file.unlink()
                    for folder in p.track(to_delete, total=len(to_delete), task_id=p_t, description="Purging"):
                        if folder.is_dir():
                            folder.rmdir()
            elif purge:
                for path in p.track(image_list, total=len(image_list), task_id=p_t):
                    hr_path, lr_path = hrlr_pair(path)
                    hr_path.unlink(missing_ok=True)
                    if lr_path is not None:
                        lr_path.unlink(missing_ok=True)

        if not overwrite:
            folders: list[Path] = [hr_folder]
            if make_lr:
                folders.append(lr_folder)

            def recurse_no_input(pth: str) -> Path:
                return recurse(Path(pth).relative_to(input_folder))

            db.add_rule(drules.ExistingRule(folders, recurse_func=recurse_no_input))

        # * Run filters
        p.log("Using rules:")
        p.log("\n".join(f" - {rule!s}" for rule in db.rules))

        total_t = p.add_task("populating df", total=None)
        p.log(len(image_list))
        abs2relative: dict[str, Path] = {str((input_folder / pth).resolve()): pth for pth in image_list}
        p.log(len(abs2relative))
        db.add_new_paths(set(abs2relative))
        db_schema = db.type_schema
        db.comply_to_schema(db_schema)
        unfinished: DataFrame = db.get_unfinished()
        if not unfinished.is_empty():
            collected: list[DataFrame] = []
            chunk: DataFrame
            p.update(total_t, total=len(unfinished))
            sub_t = p.add_task("chunk")
            for schemas, df in db.split_files_via_nulls(unfinished):
                if verbose:
                    p.log(df)
                    p.log(schemas)
                chunks = list(chunk_split(df, chunksize=cfg["chunksize"]))
                p.update(sub_t, total=len(chunks), completed=0)

                for (_, size), chunk in chunks:
                    for schema in schemas:
                        chunk = chunk.with_columns(**schema)
                    chunk = chunk.select(db_schema)
                    collected.append(chunk)
                    p.advance(sub_t)
                    p.advance(total_t, size)

            concatted: DataFrame = concat(collected, how="diagonal")
            # This breaks with datatypes like Array(3, pl.UInt32). Not sure why.
            # `pyo3_runtime.PanicException: implementation error, cannot get ref Array(Null, 0) from Array(UInt32, 3)`
            db.update(concatted)
            db.save_df()

            p.remove_task(total_t)
            p.remove_task(sub_t)
        p.update(total_t, completed=len(unfinished), total=len(unfinished))

        f_t = p.add_task("filtering", total=1)

        filtered = db.filter(set(abs2relative), sort_col=sort_by)
        image_list = [abs2relative[image] for image in filtered]
        p.update(f_t, total=1, completed=1)

        if not check_for_images(image_list):
            return 0
        if simulate:
            p.log(f"Simulated. {len(image_list)} images remain.")
            return 0

        # * convert files. Finally!
        try:
            pargs: list[Scenario] = [
                Scenario(
                    (input_folder / path).resolve(),
                    *hrlr_pair(path),
                    scale,
                )
                for path in image_list
            ]
            with Pool(threads) as pool:
                for file in p.track(pool.imap(fileparse, pargs, chunksize=chunksize), total=len(image_list)):
                    if verbose:
                        p.log(repr(file))
        except KeyboardInterrupt:
            print(-1, "KeyboardInterrupt")
            return 1

        return 0


if __name__ == "__main__":
    freeze_support()
    app()
