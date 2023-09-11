import os
from dataclasses import dataclass
from itertools import chain
from multiprocessing import Pool, cpu_count, freeze_support
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import cv2
import polars as pl
import typer
from cfg_param_wrapper import CfgDict, wrap_config
from polars import DataFrame, col
from rich import print as rprint
from tqdm import tqdm
from typer import Option
from typing_extensions import Annotated

import src.datarules.data_rules as drules
import src.datarules.image_rules as irules
from src.datarules.custom_toml import TomlCustomCommentDecoder, TomlCustomCommentEncoder
from src.datarules.dataset_builder import DatasetBuilder, chunk_split
from util.file_list import get_file_list, to_recursive
from util.print_funcs import RichStepper, ipbar

if TYPE_CHECKING:
    from collections.abc import Generator

    import numpy as np

CPU_COUNT = int(cpu_count())
app = typer.Typer()


@dataclass
class Scenario:
    """A scenario which fileparse parses and creates hr/lr pairs with."""

    relative_path: Path
    absolute_path: Path
    resolved_path: Path
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
    image: np.ndarray[int, np.dtype[np.generic]] = cv2.imread(str(dfile.absolute_path), cv2.IMREAD_UNCHANGED)
    scale = float(dfile.scale)
    image = image[0 : int((image.shape[0] // scale) * scale), 0 : int((image.shape[1] // scale) * scale)]
    mtime: float = dfile.absolute_path.stat().st_mtime
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
    stat: Annotated[bool, Option("--stat", "-s", help="use statrule", rich_help_panel="rules")] = False,
    res: Annotated[bool, Option("--res", "-r", help="use resrule", rich_help_panel="rules")] = False,
    hsh: Annotated[bool, Option("--hash", "-h", help="use hashrule", rich_help_panel="rules")] = False,
    chn: Annotated[bool, Option("--channel", "-c", help="use channelrule", rich_help_panel="rules")] = False,
    blw: Annotated[bool, Option("--bwlist", "-b", help="use blacknwhitelistrule", rich_help_panel="rules")] = False,
    lim: Annotated[bool, Option("--limit", "-l", help="limit num of files", rich_help_panel="rules")] = False,
) -> int:
    """Does all the heavy lifting"""
    s: RichStepper = RichStepper(loglevel=1, step=-1)
    s.next("Settings: ")
    cfg = CfgDict(
        config_path,
        {
            "save_interval": 60,
            "chunksize": 100,
            "filepath": "filedb.feather",
        },
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
            [
                drules.StatRule,
                irules.ResRule,
                irules.HashRule,
                irules.ChannelRule,
                drules.BlacknWhitelistRule,
                drules.TotalLimitRule,
            ]
        )
        cfg.update(db.generate_config()).save()
        print(f"{config_path} created. edit it and restart this program.")
        return 0
    cfg.load()

    def check_for_images(image_list: list[Path]) -> bool:
        if not image_list:
            s.print(-1, "No images left to process")
            return False
        return True

    def recurse(path: Path):
        return to_recursive(path, recursive, underscores)

    if not input_folder:
        rprint("Please select a directory.")
        return 1
    if not input_folder.exists():
        rprint(f"Folder {input_folder} does not exist.")
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

    rules = []
    producers = set()

    if stat:
        rules.append(drules.StatRule)
        producers.add(drules.FileInfoProducer())
    if res or chn:
        producers.add(irules.ImShapeProducer())
    if chn:
        rules.append(irules.ChannelRule)
    if res:
        rules.append(irules.ResRule)
    if hsh:
        rules.append(irules.HashRule(cfg["hashing"]["resolver"]))
        producers.add(irules.HashProducer(cfg["hashing"]["hasher"]))
    if blw:
        rules.append(drules.BlacknWhitelistRule)
    if lim:
        rules.append(drules.TotalLimitRule)
    db.add_rules(rules)
    db.add_producers(producers)
    db.fill_from_config(cfg)

    # * Gather images
    s.next("Gathering images...")
    available_extensions: list[str] = extensions.split(",")
    s.print(f"Searching extensions: {available_extensions}")
    file_list: Generator[Path, None, None] = get_file_list(input_folder, *(f"*.{ext}" for ext in available_extensions))
    image_list: list[Path] = [x.relative_to(input_folder) for x in sorted(file_list)]

    s.print(f"Gathered {len(image_list)} images")

    # * Purge existing images
    if purge_all:
        # This could be cleaner
        to_delete = set(chain(get_file_list(hr_folder, "*"), get_file_list(lr_folder, "*")))
        if to_delete:
            s.next("Purging...")
            for file in ipbar(to_delete, total=len(to_delete)):
                if file.is_file():
                    file.unlink()
            for folder in ipbar(to_delete, total=len(to_delete)):
                if folder.is_dir():
                    folder.rmdir()
    elif purge:
        s.next("Purging...")
        for path in ipbar(image_list, total=len(image_list)):
            hr_path, lr_path = hrlr_pair(path)
            hr_path.unlink(missing_ok=True)
            if lr_path is not None:
                lr_path.unlink(missing_ok=True)

    if not overwrite:
        folders: list[Path] = [hr_folder]
        if make_lr:
            folders.append(lr_folder)
        db.add_rule(drules.ExistingRule(folders, recurse_func=recurse))

    # * Run filters
    s.next("Using: ")
    s.print(*[f" - {rule!s}" for rule in db.rules])

    s.print("Populating df...")

    abs2relative: dict[str, Path] = {str((input_folder / pth).resolve()): pth for pth in image_list}
    db.add_new_paths(set(abs2relative))
    db_schema = db.type_schema
    db.comply_to_schema(db_schema)
    unfinished: DataFrame = db.get_unfinished()
    if not unfinished.is_empty():
        print(unfinished)
        collected: list[DataFrame] = []
        chunk: DataFrame
        with (
            tqdm(desc="Gathering...", unit="file", total=len(unfinished)) as total_t,
            tqdm(unit="chunk", total=len(unfinished) // chunksize) as sub_t,
        ):
            for schemas, df in db.split_files_via_nulls(unfinished):
                chunks = list(chunk_split(df, chunksize=cfg["chunksize"]))
                sub_t.total = len(chunks)
                sub_t.update(-sub_t.n)
                for (idx, size), chunk in chunks:
                    for schema in schemas:
                        chunk = chunk.with_columns(**schema)
                    chunk = chunk.select(db_schema)
                    collected.append(chunk)

                    sub_t.set_postfix_str(str(idx))
                    sub_t.update()
                    total_t.update(size)
        concatted = pl.concat(collected, how="diagonal")
        # This breaks with datatypes like Array(3, pl.UInt32). Not sure why.
        # `pyo3_runtime.PanicException: implementation error, cannot get ref Array(Null, 0) from Array(UInt32, 3)`
        db.update(concatted)
        db.save_df()
    exit()

    s.print("Filtering...")
    filtered = db.filter(set(abs2relative), sort_col=sort_by)
    image_list = [abs2relative[image] for image in filtered]

    if not check_for_images(image_list):
        return 0
    if simulate:
        s.next(f"Simulated. {len(image_list)} images remain.")
        return 0

    # * convert files. Finally!
    try:
        pargs: list[Scenario] = [
            Scenario(path, input_folder / path, (input_folder / path).resolve(), *hrlr_pair(path), scale)
            for path in image_list
        ]
        print(len(pargs))
        with Pool(threads) as p, tqdm(p.imap(fileparse, pargs, chunksize=chunksize), total=len(image_list)) as t:
            for file in t:
                if verbose:
                    print()
                    print(db.__df.filter(col("path") == str(file.resolved_path)))  # I can't imagine this is fast
                    if file.lr_path is not None:
                        rprint(f"├hr -> '{file.hr_path}'")
                        rprint(f"└lr -> '{file.lr_path}'")
                    else:
                        rprint(f"└hr -> '{file.hr_path}'")

    except KeyboardInterrupt:
        print(-1, "KeyboardInterrupt")
        return 1
    return 0


if __name__ == "__main__":
    freeze_support()
    app()
