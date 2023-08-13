import os
from dataclasses import dataclass
from enum import Enum
from multiprocessing import Pool, cpu_count, freeze_support
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import cv2
import typer
from cfg_param_wrapper import CfgDict, wrap_config
from polars import col
from rich import print as rprint
from tqdm import tqdm
from typer import Option
from typing_extensions import Annotated

from src.datafilters.custom_toml import TomlCustomCommentDecoder, TomlCustomCommentEncoder
from src.datafilters.data_filters import BlacknWhitelistFilter, ExistingFilter, StatFilter
from src.datafilters.dataset_builder import DatasetBuilder
from src.datafilters.external_filters import ChannelFilter, HashFilter, ResFilter
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

    if not os.path.exists(dfile.hr_path):
        cv2.imwrite(str(dfile.hr_path), image)
        os.utime(str(dfile.hr_path), (mtime, mtime))

    if dfile.lr_path is not None and not os.path.exists(dfile.lr_path):
        cv2.imwrite(str(dfile.lr_path), cv2.resize(image, (int(image.shape[1] // scale), int(image.shape[0] // scale))))
        os.utime(str(dfile.lr_path), (mtime, mtime))

    return dfile


class LimitModes(str, Enum):
    """Modes to limit the output images based on a timestamp."""

    BEFORE = "before"
    AFTER = "after"


config = CfgDict("config.toml", save_mode="toml")


@app.command()
@wrap_config(config)
def main(
    input_folder: Annotated[Path, Option(help="the folder to scan.")] = Path(),
    scale: Annotated[int, Option(help="the scale to downscale.")] = 4,
    extension: Annotated[Optional[str], Option(help="export extension.")] = None,
    extensions: Annotated[str, Option(help="extensions to search for. (split with commas)")] = "webp,png,jpg",
    config_path: Annotated[Path, Option(help="Where the filter config is placed.")] = Path("database_config.toml"),
    recursive: Annotated[bool, Option(help="preserves the tree hierarchy.", rich_help_panel="modifiers")] = False,
    convert_spaces: Annotated[
        bool,
        Option(
            help="Whether to replace spaces with underscores when creating the output.", rich_help_panel="modifiers"
        ),
    ] = False,
    threads: Annotated[int, Option(help="number of threads for multiprocessing.", rich_help_panel="modifiers")] = int(
        CPU_COUNT * (3 / 4)
    ),
    chunksize: Annotated[
        int, Option(help="number of images to run with one thread per pool chunk", rich_help_panel="modifiers")
    ] = 5,
    limit: Annotated[
        Optional[int], Option(help="only gathers a given number of images.", rich_help_panel="modifiers")
    ] = None,
    limit_mode: Annotated[
        LimitModes, Option(help="which order the limiter is activated.", rich_help_panel="modifiers")
    ] = LimitModes.BEFORE,
    simulate: Annotated[
        bool, Option(help="skips the conversion step. Used for debugging.", rich_help_panel="modifiers")
    ] = False,
    purge: Annotated[
        bool, Option(help="deletes output corresponding to input files.", rich_help_panel="modifiers")
    ] = False,
    purge_all: Annotated[
        bool, Option(help="Same as above, but deletes *everything*.", rich_help_panel="modifiers")
    ] = False,
    make_lr: Annotated[bool, Option(help="whether to make an LR folder.", rich_help_panel="modifiers")] = True,
    overwrite: Annotated[
        bool,
        Option(help="Skips checking existing files, overwrites existing files.", rich_help_panel="modifiers"),
    ] = False,
    verbose: Annotated[
        bool, Option(help="Prints the files when they are fully converted.", rich_help_panel="modifiers")
    ] = False,
    sort_by: Annotated[
        str,
        Option(
            help="Which column in the database to sort by. It must be in the database.", rich_help_panel="modifiers"
        ),
    ] = "path",
    stat: Annotated[bool, Option("--stat", "-s", help="use statfilter", rich_help_panel="filters")] = False,
    res: Annotated[bool, Option("--res", "-r", help="use resfilter", rich_help_panel="filters")] = False,
    hsh: Annotated[bool, Option("--hash", "-h", help="use hashfilter", rich_help_panel="filters")] = False,
    chn: Annotated[bool, Option("--channel", "-c", help="use channelfilter", rich_help_panel="filters")] = False,
    blw: Annotated[
        bool, Option("--blackwhitelist", "-b", help="use blacknwhitelistfilter", rich_help_panel="filters")
    ] = False,
) -> int:
    """Does all the heavy lifting"""
    s: RichStepper = RichStepper(loglevel=1, step=-1)
    s.next("Settings: ")
    cfg = CfgDict(
        config_path,
        {
            "trim": True,
            "trim_age_limit": 60 * 60 * 24 * 7,
            "trim_check_exists": True,
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

    db = DatasetBuilder(origin=str(input_folder), db_path=Path(cfg["filepath"]))
    if not config_path.exists():
        db.add_filters([StatFilter, ResFilter, HashFilter, ChannelFilter, BlacknWhitelistFilter])
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
        return to_recursive(path, recursive, convert_spaces)

    if not input_folder or not os.path.exists(input_folder):
        rprint("Please select a directory.")
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

    filters = []
    if stat:
        filters.append(StatFilter)
    if res:
        filters.append(ResFilter)
    if hsh:
        filters.append(HashFilter)
    if chn:
        filters.append(ChannelFilter)
    if blw:
        filters.append(BlacknWhitelistFilter)
    db.add_filters(filters)
    db.fill_from_config(cfg)

    # * Gather images
    s.next("Gathering images...")
    available_extensions: list[str] = extensions.split(",")
    s.print(f"Searching extensions: {available_extensions}")
    file_list: Generator[Path, None, None] = get_file_list(
        *[input_folder / "**" / f"*.{ext}" for ext in available_extensions]
    )
    image_list: list[Path] = [x.relative_to(input_folder) for x in sorted(file_list)]
    if limit and limit == LimitModes.BEFORE:
        image_list = image_list[:limit]

    s.print(f"Gathered {len(image_list)} images")

    # * Purge existing images
    if purge_all:
        # This could be cleaner
        to_delete: set[Path] = set(get_file_list(hr_folder / "**" / "*", lr_folder / "**" / "*"))
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
        db.add_filter(ExistingFilter(folders, recurse_func=recurse))

    # * Run filters
    s.next("Using: ")
    s.print(*[f" - {str(filter_)}" for filter_ in db.filters])

    s.print("Populating df...")
    db.populate_df(
        image_list, cfg["trim"], cfg["trim_age_limit"], cfg["save_interval"], cfg["trim_check_exists"], cfg["chunksize"]
    )

    s.print("Filtering...")
    image_list = db.filter(image_list, sort_col=sort_by)

    if limit and limit_mode == LimitModes.AFTER:
        image_list = image_list[:limit]

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
                    print(db.df.filter(col("path") == str(file.resolved_path)))  # I can't imagine this is fast
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
