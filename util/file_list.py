from pathlib import Path
from glob import glob
from os import sep
from collections.abc import Generator


def get_file_list(*folders: Path) -> Generator[Path, None, None]:
    """
    Args    folders: One or more folder paths.
    Returns list[Path]: paths in the specified folders."""

    return (Path(y) for x in (glob(str(p), recursive=True) for p in folders) for y in x)


def to_recursive(path: Path, recursive: bool = False, replace_spaces: bool = False) -> Path:
    """Convert the file path to a recursive path if recursive is False
    (Also replaces spaces with underscores)
    Ex: i/path/to/image.png => i/path_to_image.png"""
    new_pth: str = str(path)
    if replace_spaces and " " in new_pth:
        new_pth = new_pth.replace(" ", "_")
    if not recursive and sep in new_pth:
        new_pth = new_pth.replace(sep, "_")
    return Path(new_pth)
