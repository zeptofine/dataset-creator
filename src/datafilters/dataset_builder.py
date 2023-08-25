import inspect
import os
import warnings
from collections.abc import Collection, Iterable
from datetime import datetime
from pathlib import Path
from typing import TypeVar

import polars as pl
from polars import DataFrame, Expr, LazyFrame
from tqdm import tqdm

from .base_filters import Column, Comparable, DataFilter, FastComparable


def current_time() -> datetime:
    return datetime.now().replace(microsecond=0)


def _time(_=None) -> datetime:
    return current_time()


T = TypeVar("T")


class DatasetBuilder:
    def __init__(self, origin: str, db_path: Path) -> None:
        super().__init__()
        self.unready_filters: dict[str, type[DataFilter]] = {}
        self.filters: list[DataFilter] = []
        self.origin: str = origin

        self.filepath: Path = db_path

        self.basic_schema: dict[str, pl.DataType | type] = {"path": str, "checkedtime": pl.Datetime}
        self.build_schema: dict[str, Expr] = {"checkedtime": pl.col("path").apply(_time)}
        self.filter_type_schema: dict[str, pl.DataType | type] = self.basic_schema.copy()
        self.columns: dict[str, Column] = {}

        if self.filepath.exists():
            self.df: DataFrame = pl.read_ipc(self.filepath, use_pyarrow=True)
        else:
            self.df: DataFrame = DataFrame(schema=self.basic_schema)

    def add_filters(self, filters: list[type[DataFilter] | DataFilter]) -> None:
        """Adds filters to the filter list. Filters will be instantiated separately."""

        for filter_ in filters:
            self.add_filter(filter_)

    def add_filter(self, filter_: type[DataFilter] | DataFilter):
        if isinstance(filter_, type):
            self.unready_filters[filter_.config_keyword] = filter_
        elif isinstance(filter_, DataFilter):
            if filter_ not in self.filters:
                self.filters.append(filter_)

                for column in filter_.schema:
                    self.add_schema(column)
        else:
            raise self.NotFilterError(filter_)

    def fill_from_config(self, cfg: dict[str, dict], no_warn=False):
        if not len(self.unready_filters):
            raise self.UnreadyFilterError()

        for kwd, dct in cfg.items():
            if kwd in self.unready_filters:
                filter_ = self.unready_filters.pop(kwd)
                sig: inspect.Signature = inspect.signature(filter_)

                params = {k: v for k, v in dct.items() if k in sig.parameters and k != "self"}
                self.add_filter(filter_(**params))
        if len(self.unready_filters) and not no_warn:
            warnings.warn(f"{self.unready_filters} remain unfilled from config.")

    def generate_config(self) -> dict:
        return {name: filter_.get_cfg() for name, filter_ in self.unready_filters.items()}

    def add_schema(self, col: Column, overwrite=False):
        self.columns[col.name] = col
        if col.build_method is not None:
            assert col.name not in self.build_schema, f"Column is already in build_schema ({self.build_schema})"
            self.build_schema[col.name] = col.build_method
        if col.name not in self.filter_type_schema:
            self.filter_type_schema[col.name] = col.dtype

    def populate_df(
        self,
        lst: Iterable[Path],
        trim: bool = True,
        trim_age_limit: int = 60 * 60 * 24 * 7,
        use_tqdm=True,
        save_interval: int = 60,
        trim_check_exists: bool = True,
        chunksize: int = 100,
    ):
        assert self.filters, "No filters specified"
        if trim and len(self.df):
            now: datetime = current_time()
            cond: Expr = now - pl.col("checkedtime") < datetime.fromtimestamp(trim_age_limit)
            if trim_check_exists:
                cond &= pl.col("path").apply(os.path.exists)
            self.df = self.df.filter(cond)

        from_full_to_relative: dict[str, Path] = self.get_absolutes(lst)
        if new_paths := set(from_full_to_relative) - set(self.df.get_column("path")):
            self.df = pl.concat((self.df, DataFrame({"path": list(new_paths)})), how="diagonal")

        for filter_ in self.filters:
            filter_.filedict = from_full_to_relative
        self.df = self._comply_to_schema(self.df, self.filter_type_schema)

        search_cols: set[str] = {*self.build_schema, *self.basic_schema}
        updated_df: DataFrame = self.df.with_columns(self.filter_type_schema)
        unfinished: DataFrame = updated_df.filter(
            pl.any(pl.col(col).is_null() for col in updated_df.columns if col in search_cols)
        )
        if len(unfinished):

            def trigger_save(save_timer: datetime, collected: list[DataFrame]) -> tuple[datetime, list[DataFrame]]:
                if ((new_time := datetime.now()) - save_timer).total_seconds() > save_interval:
                    data = pl.concat(collected, how="diagonal")
                    self.df = self.df.update(data, on="path")
                    self.save_df()
                    return new_time, []
                return save_timer, collected

            def get_nulls_chunks_expr():
                for nulls, group in unfinished.with_columns(self.filter_type_schema).groupby(
                    *(pl.col(col).is_not_null() for col in self.build_schema)
                ):
                    yield (
                        self._split_into_chunks(group, chunksize=chunksize),
                        {
                            col: expr
                            for truth, (col, expr) in zip(nulls, self.build_schema.items())  # type: ignore
                            if not truth
                        },
                    )

            save_timer: datetime = datetime.now()
            collected: list[DataFrame] = []
            if not use_tqdm:
                for subgroups, expr in get_nulls_chunks_expr():
                    for _, subgroup in subgroups:
                        collected.append(subgroup.with_columns(**expr))
                        save_timer, collected = trigger_save(save_timer, collected)
            else:
                with (
                    tqdm(desc="Gathering file info...", unit="file", total=len(unfinished)) as total_t,
                    tqdm(desc="Processing chunks...", unit="chunk", total=len(unfinished) // chunksize) as sub_t,
                ):
                    for subgroups, expr in get_nulls_chunks_expr():
                        subgroups = list(subgroups)
                        sub_t.total = len(subgroups)
                        sub_t.update(-sub_t.n)
                        total_t.set_postfix_str(str(set(expr)))

                        for idx, subgroup in subgroups:
                            collected.append(subgroup.with_columns(**expr))
                            save_timer, collected = trigger_save(save_timer, collected)

                            sub_t.set_postfix_str(str(idx))
                            sub_t.update()
                            total_t.update(len(subgroup))
            self.df = self.df.update(pl.concat(collected, how="diagonal"), on="path")
            self.save_df()
        return

    def filter(self, lst, sort_col="path", ignore_missing_columns=False) -> Iterable[Path]:
        assert (
            sort_col in self.df.columns
        ), f"the column '{sort_col}' is not in the database. Available columns: {self.df.columns}"
        if len(self.unready_filters):
            warnings.warn(f"{len(self.unready_filters)} filters are not initialized and will not be populated")

        if (missing_requirements := set(self.columns) - set(self.build_schema)) and not ignore_missing_columns:
            raise self.MissingRequirementError(missing_requirements)

        from_full_to_relative: dict[str, Path] = self.get_absolutes(lst)
        paths: set[str] = set(from_full_to_relative.keys())

        vdf: LazyFrame = self.df.lazy().filter(pl.col("path").is_in(paths))
        for dfilter in self.filters:
            if isinstance(dfilter, FastComparable):
                vdf = vdf.filter(dfilter.fast_comp())
            elif isinstance(dfilter, Comparable):
                vdf = (
                    (c := vdf.collect())
                    .filter(
                        pl.col("path").is_in(
                            dfilter.compare(
                                set(c.get_column("path")),
                                self.df.select(pl.col("path"), *[pl.col(col.name) for col in dfilter.schema]),
                            )
                        )
                    )
                    .lazy()
                )

        return (from_full_to_relative[p] for p in vdf.sort(sort_col).collect().get_column("path"))

    def save_df(self) -> None:
        """saves the dataframe to self.filepath"""
        self.df.write_ipc(self.filepath)

    def get_absolutes(self, lst: Iterable[Path]) -> dict[str, Path]:
        return {(str((self.origin / pth).resolve())): pth for pth in lst}  # type: ignore

    def get_path_data(self, pths: Collection[str]):
        return self.df.filter(pl.col("path").is_in(pths))

    @staticmethod
    def _comply_to_schema(data_frame: DataFrame, schema) -> DataFrame:
        """adds columns from the schema to the dataframe. (not in-place)"""
        return pl.concat((data_frame, DataFrame(schema=schema)), how="diagonal")

    @staticmethod
    def _split_into_chunks(df: DataFrame, chunksize: int, column="_idx"):
        return (
            ((idx, len(part)), part.drop(column))
            for idx, part in df.with_row_count(column)
            .with_columns(pl.col(column) // chunksize)
            .groupby(column, maintain_order=True)
        )

    class NotFilterError(TypeError):
        def __init__(self, obj: object):
            super().__init__(f"{obj} is not a valid filter")

    class UnreadyFilterError(KeyError):
        def __init__(self):
            super().__init__("Unready filters is empty")

    class MissingRequirementError(KeyError):
        def __init__(self, reqs: Iterable[str]):
            super().__init__(f"Possibly missing columns: {reqs}")

    def __enter__(self, *args, **kwargs):
        self.__init__(*args, **kwargs)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass
