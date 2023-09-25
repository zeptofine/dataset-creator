import time
from collections.abc import Generator, Iterable
from os import get_terminal_size
from typing import Callable, TypeVar

from rich import print as rprint


def byte_format(size, leading: int = 3, trailing: int = 4, suffix="B") -> str:
    """modified version of: https://stackoverflow.com/a/1094933"""
    if isinstance(size, str):
        size = "".join([val for val in size if val.isnumeric()])
    size = str(size)
    if size != "":
        size = int(size)
        unit = ""
        for unit in ["", "Ki", "Mi", "Gi", "Ti"]:
            if abs(size) < 2**10:
                return f"{size:{leading + trailing + 1}.{trailing}f}{unit}{suffix}"
            size /= 2**10
        return f"{size:3.1f}{unit}{suffix}"
    return f"N/A{suffix}"


def pbar(iteration: int, total: int, length=20, fill="#", nullp="-", corner="[]", pref="", suff="") -> str:
    filled: int = (length * iteration) // total
    #    [#############################]
    c1, c2 = "\033[92m", "\033[93m"
    return f"{pref}{c1}{corner[0]}{c2}{(fill*length)[:filled]:{nullp}<{length}}{c1}{corner[1]}\033[0m{suff}"


def isbar(iteration, total, suff="", **kwargs):
    return f"{pbar(iteration, total, **kwargs)} {iteration:{len(str(total))}}/{total} {suff}"


T = TypeVar("T")


def ipbar(
    iterable: Iterable[T],
    total=100,
    refresh_interval=0.25,
    end="\r",
    very_end="\n",
    clear=False,
    print_item=False,
    **kwargs,
) -> Generator[T, None, None]:
    _time: float = time.time()
    for i, obj in enumerate(iterable):
        yield obj
        newtime = time.time()
        if newtime - _time > refresh_interval:  # refresh interval
            output = isbar(i + 1, total, **kwargs)
            if print_item:
                output += f" {obj!s}"
            print(f"\033[K{output}", end=end)
            _time = newtime
    print(isbar(total, total, **kwargs), end="\033[2K\r" if clear else very_end)


def thread_status(pid: int, item: str = "", extra: str = "", item_size: int | None = None):
    """I don't know whether I should keep this or not. Don't really need it anymore"""
    item_size = item_size or get_terminal_size().columns
    message = f"{pid}: {item}".ljust(item_size)[: item_size - len(extra)] + extra
    print(("\n" * pid) + message + ("\033[A" * pid), end="\r")


class Timer:
    def __init__(self, timestamp: int | None = None):
        self.time = timestamp or time.perf_counter()

    def log(self, msg):
        """print and resets time"""
        return self.poll(msg).reset()

    def poll(self, msg=""):
        """print without resetting time"""
        print(f"{time.perf_counter() - self.time}: {msg}")
        return self

    def reset(self):
        """resets time"""
        self.time = time.perf_counter()
        return self.time

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return str((time.perf_counter()) - self.time)
