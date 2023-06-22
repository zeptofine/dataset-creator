import os
from collections.abc import Callable, Collection, Iterable

import imagehash
import imagesize
from polars import List, col
from PIL import Image
from polars import DataFrame, Expr

from .base_filters import Comparable, DataFilter, FastComparable


class ResFilter(DataFilter, FastComparable):
    """A filter checking the size of an image."""

    def __init__(self, minsize: int | None, maxsize: int | None, crop_mod: bool, scale: int) -> None:
        super().__init__()
        self.column_schema = {"resolution": List(int)}
        self.build_schema = {"resolution": col("path").apply(imagesize.get)}
        self.min: int | None = minsize
        self.max: int | None = maxsize
        self.crop: bool = crop_mod
        self.scale: int = scale

    def fast_comp(self) -> Expr | bool:
        if self.crop:
            return col("resolution").apply(lambda lst: self.is_valid(map(self.resize, lst)))
        return col("resolution").apply(lambda lst: all(dim % self.scale == 0 for dim in lst) and self.is_valid(lst))

    def is_valid(self, lst: Iterable[int]) -> bool:
        lst = set(lst)
        return not ((self.min and min(lst) < self.min) or (self.max and max(lst) > self.max))

    def resize(self, i: int) -> int:
        return (i // self.scale) * self.scale


class ChannelFilter(DataFilter, FastComparable):
    """Checks the number of channels in the image."""

    def __init__(self, channel_num: int | None, strict: bool = False) -> None:
        super().__init__()
        self.column_schema = {"channels": int}
        self.build_schema = {"channels": col("path").apply(self.get_channels)}

        self.channel_num: int | None = channel_num
        self.strict: bool = strict

    def get_channels(self, pth: str) -> int:
        return len(Image.open(pth).getbands())

    def fast_comp(self) -> Expr | bool:
        if not self.channel_num:
            return True
        if self.strict:
            return col("channels") == self.channel_num
        return col("channels") <= self.channel_num


IMHASH_TYPES: dict[str, Callable] = {
    "average": imagehash.average_hash,
    "crop_resistant": imagehash.crop_resistant_hash,
    "color": imagehash.colorhash,
    "dhash": imagehash.dhash,
    "dhash_vertical": imagehash.dhash_vertical,
    "phash": imagehash.phash,
    "phash_simple": imagehash.phash_simple,
    "whash": imagehash.whash,
    "whash-db4": lambda img: imagehash.whash(img, mode="db4"),
}


class HashFilter(DataFilter, Comparable):
    def __init__(self, hash_choice: str = "average", resolver: str = "newest") -> None:
        super().__init__()
        self.column_schema = {"hash": str}  # type: ignore
        self.build_schema: dict[str, Expr] = {"hash": col("path").apply(self._hash_img)}

        imhash_resolvers: dict[str, Callable] = {
            "ignore_all": HashFilter._ignore_all,
            "newest": HashFilter._accept_newest,
            "oldest": HashFilter._accept_oldest,
            "size": HashFilter._accept_biggest,
        }

        if hash_choice not in IMHASH_TYPES:
            raise KeyError(f"{hash_choice} is not in IMHASH_TYPES")
        if resolver not in imhash_resolvers:
            raise KeyError(f"{resolver} is not in IMHASH_RESOLVERS")

        self.hasher: Callable[[Image.Image], imagehash.ImageHash] = IMHASH_TYPES[hash_choice]
        self.resolver: Callable[[], Expr | bool] = imhash_resolvers[resolver]

    def compare(self, lst: Collection, cols: DataFrame) -> set:
        applied: DataFrame = (
            cols.filter(col("hash").is_in(cols.filter(col("path").is_in(lst)).select(col("hash")).unique().to_series()))
            .groupby("hash")
            .apply(lambda df: df.filter(self.resolver()) if len(df) > 1 else df)
        )

        resolved_paths = set(applied.select(col("path")).to_series())
        return resolved_paths

    def _hash_img(self, pth) -> str:
        return str(self.hasher(Image.open(pth)))

    @staticmethod
    def _ignore_all() -> Expr | bool:
        return False

    @staticmethod
    def _accept_newest() -> Expr:
        return col("modifiedtime") == col("modifiedtime").max()

    @staticmethod
    def _accept_oldest() -> Expr:
        return col("modifiedtime") == col("modifiedtime").min()

    @staticmethod
    def _accept_biggest() -> Expr:
        sizes: Expr = col("path").apply(lambda p: os.stat(str(p)).st_size)
        return sizes == sizes.max()
