from collections.abc import Generator
from os import sep
from pathlib import Path


def get_file_list(folder, *patterns: str) -> Generator[Path, None, None]:
    """
    Args    folders: One or more folder paths.
    Returns list[Path]: paths in the specified folders."""

    return (y for pattern in patterns for y in folder.rglob(pattern))


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
