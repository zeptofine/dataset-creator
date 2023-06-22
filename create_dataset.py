import os
from dataclasses import dataclass
from enum import Enum
from multiprocessing import Pool, cpu_count, freeze_support
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import cv2
import dateutil.parser as timeparser
import typer
from cfg_param_wrapper import CfgDict, wrap_config
from polars import col
from rich import print as rprint
from tqdm import tqdm
from typer import Option
from typing_extensions import Annotated

from dataset_filters.data_filters import BlacknWhitelistFilter, ExistingFilter, StatFilter
from dataset_filters.dataset_builder import DatasetBuilder
from dataset_filters.external_filters import ChannelFilter, HashFilter, ResFilter
from util.file_list import get_file_list, to_recursive
from util.print_funcs import RichStepper, ipbar

if TYPE_CHECKING:
    from collections.abc import Generator
    from datetime import datetime

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


class HashModes(str, Enum):
    """Modes to hash the images to compare."""

    AVERAGE = "average"
    CROP_RESISTANT = "crop_resistant"
    COLOR = "color"
    DHASH = "dhash"
    DHASH_VERTICAL = "dhash_vertical"
    PHASH = "phash"
    PHASH_SIMPLE = "phash_simple"
    WHASH = "whash"
    WHASH_DB4 = "whash-db4"


class HashChoices(str, Enum):
    """How to decide which image to keep / remove."""

    IGNORE_ALL = "ignore_all"
    NEWEST = "newest"
    OLDEST = "oldest"
    SIZE = "size"


config = CfgDict("config.toml", save_mode="toml")


@app.command()
@wrap_config(config)
def main(
    input_folder: Annotated[Path, Option(help="the folder to scan.")] = Path(),
    scale: Annotated[int, Option(help="the scale to downscale.")] = 4,
    extension: Annotated[Optional[str], Option(help="export extension.")] = None,
    extensions: Annotated[str, Option(help="extensions to search for. (split with commas)")] = "webp,png,jpg",
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
    # BlacknWhitelistFilter
    whitelist: Annotated[
        Optional[str], Option(help="only allows paths with the given strings.", rich_help_panel="filters")
    ] = None,
    blacklist: Annotated[
        Optional[str], Option(help="Excludes paths with the given strings.", rich_help_panel="filters")
    ] = None,
    list_separator: Annotated[str, Option(help="separator for the white/blacklists.", rich_help_panel="filters")] = ",",
    # ResFilter
    minsize: Annotated[Optional[int], Option(help="minimum size an image must be.", rich_help_panel="filters")] = None,
    maxsize: Annotated[Optional[int], Option(help="maximum size an image can be.", rich_help_panel="filters")] = None,
    crop_mod: Annotated[
        bool,
        Option(help="changes the res filter to crop the image to be divisible by scale", rich_help_panel="filters"),
    ] = False,
    # StatFilter
    before: Annotated[
        Optional[str], Option(help="only uses files before a given date", rich_help_panel="filters")
    ] = None,
    after: Annotated[Optional[str], Option(help="only uses after a given date.", rich_help_panel="filters")] = None,
    # ^^ these will be parsed with dateutil.parser ^^
    # ChannelFilter
    channel_num: Annotated[
        Optional[int], Option(help="number of channels an image must have.", rich_help_panel="filters")
    ] = None,
    # HashFilter
    hash_images: Annotated[
        bool, Option(help="Removes perceptually similar images.", rich_help_panel="filters")
    ] = False,
    hash_mode: Annotated[
        HashModes,
        Option(
            help="How to hash the images. read https://github.com/JohannesBuchner/imagehash for more info",
            rich_help_panel="filters",
        ),
    ] = HashModes.AVERAGE,
    hash_choice: Annotated[
        HashChoices, Option(help="What to do in the occurance of a hash conflict.", rich_help_panel="filters")
    ] = HashChoices.IGNORE_ALL,
) -> int:
    """Does all the heavy lifting"""
    s: RichStepper = RichStepper(loglevel=1, step=-1)
    s.next("Settings: ")

    db = DatasetBuilder(origin=str(input_folder))

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

    dtafter: datetime | None = None
    dtbefore: datetime | None = None
    if before or after:
        try:
            if after:
                dtafter = timeparser.parse(str(after))
            if before:
                dtbefore = timeparser.parse(str(before))
            if dtafter is not None and dtafter is not None:
                if dtafter > dtbefore:  # type: ignore
                    raise timeparser.ParserError(f"{dtbefore} (--before) is older than {dtafter} (--after)!")

            s.print(f"Filtering by time ({dtbefore} <= x <= {dtafter})")

        except timeparser.ParserError as err:
            s.set(-9).print(str(err))
            return 1

    # * {White,Black}list option
    if whitelist or blacklist:
        lists: list[list[str] | None] = [None, None]
        if whitelist:
            lists[0] = whitelist.split(list_separator)
        if blacklist:
            lists[1] = blacklist.split(list_separator)
        db.add_filters(BlacknWhitelistFilter(*lists))

    if (minsize and minsize <= 0) or (maxsize and maxsize <= 0):
        print("selected minsize and/or maxsize is invalid")
    if minsize or maxsize:
        s.print(f"Filtering by size ({minsize} <= x <= {maxsize})")

    if dtbefore or dtafter:
        db.add_filters(StatFilter(dtbefore, dtafter))
    if scale != 1 or minsize or maxsize:
        db.add_filters(ResFilter(minsize, maxsize, crop_mod, scale))
    if hash_images:
        db.add_filters(HashFilter(hash_mode, hash_choice))
    if channel_num:
        db.add_filters(ChannelFilter(channel_num))

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
        db.add_filters(ExistingFilter(*folders, recurse_func=recurse))

    # * Run filters
    s.next("Populating df...")
    db.populate_df(image_list)

    s.next("Filtering using:")
    s.print(*[f" - {str(filter_)}" for filter_ in db.filters])
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
