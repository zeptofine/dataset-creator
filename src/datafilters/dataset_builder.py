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

        if os.path.exists(self.filepath):
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
            raise TypeError(f"{filter_} is not a filter.")

    def fill_from_config(self, cfg: dict[str, dict], no_warn=False):
        if not len(self.unready_filters):
            raise KeyError("Unready filters is empty")

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

        from_full_to_relative: dict[str, Path] = self.absolute_dict(lst)
        # add new paths to the dataframe with missing data
        existing_paths = set(self.df.select(pl.col("path")).to_series())
        new_paths: list[str] = [path for path in from_full_to_relative if path not in existing_paths]
        if new_paths:
            self.df = pl.concat(
                [self.df, DataFrame({"path": new_paths})],
                how="diagonal",
            )

        for filter_ in self.filters:
            filter_.filedict = from_full_to_relative
        self.df = self._make_schema_compliant(self.df, self.filter_type_schema)
        updated_df: DataFrame = self.df.with_columns(self.filter_type_schema)
        search_cols: set[str] = {*self.build_schema, *self.basic_schema}

        unfinished: DataFrame = updated_df.filter(
            pl.any(pl.col(col).is_null() for col in updated_df.columns if col in search_cols)
        )
        if len(unfinished):
            with (
                tqdm(desc="Gathering file info...", unit="file", total=len(unfinished)) as total_t,
                tqdm(desc="Processing chunks...", unit="chunk", total=len(unfinished) // chunksize) as sub_t,
            ):
                save_timer = datetime.now()
                collected_data: DataFrame = DataFrame(schema=self.filter_type_schema)
                for nulls, group in unfinished.with_columns(self.filter_type_schema).groupby(
                    *(pl.col(col).is_not_null() for col in self.build_schema)
                ):
                    group_expr = {
                        col: expr
                        for truth, (col, expr) in zip(nulls, self.build_schema.items())  # type: ignore
                        if not truth
                    }
                    subgroups = list(self._split_into_chunks(group, chunksize=chunksize))
                    total_t.set_postfix_str(str(tuple(group_expr.keys())))
                    sub_t.total = len(subgroups)
                    sub_t.n = 0

                    for idx, subgroup in subgroups:
                        new_data: DataFrame = subgroup.with_columns(**group_expr)
                        collected_data = pl.concat((collected_data, new_data), how="diagonal")
                        if ((new_time := datetime.now()) - save_timer).total_seconds() > save_interval:
                            self.df = self.df.update(collected_data, on="path")
                            self.save_df()
                            collected_data = collected_data.clear()
                            save_timer = new_time
                        sub_t.set_postfix_str(str(idx))
                        sub_t.update(1)
                        total_t.update(len(subgroup))
                self.df = self.df.update(collected_data, on="path").rechunk()
            self.save_df()
        return

    def filter(self, lst, sort_col="path", ignore_missing_columns=False) -> Iterable[Path]:
        assert (
            sort_col in self.df.columns
        ), f"the column '{sort_col}' is not in the database. Available columns: {self.df.columns}"
        if len(self.unready_filters):
            warnings.warn(f"{len(self.unready_filters)} filters are not initialized and will not be populated")

        if (missing_requirements := set(self.columns) - set(self.build_schema)) and not ignore_missing_columns:
            raise ValueError(
                f"the following columns are required but may not be in the database: {missing_requirements}"
            )

        from_full_to_relative: dict[str, Path] = self.absolute_dict(lst)
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
                                set(c.select(pl.col("path")).to_series()),
                                self.df.select(pl.col("path"), *[pl.col(col.name) for col in dfilter.schema]),
                            )
                        )
                    )
                    .lazy()
                )

        return (from_full_to_relative[p] for p in vdf.sort(sort_col).select(pl.col("path")).collect().to_series())

    def save_df(self) -> None:
        """saves the dataframe to self.filepath"""
        self.df.write_ipc(self.filepath)

    def absolute_dict(self, lst: Iterable[Path]) -> dict[str, Path]:
        return {(str((self.origin / pth).resolve())): pth for pth in lst}  # type: ignore

    def get_path_data(self, pths: Collection[str]):
        return self.df.filter(pl.col("path").is_in(pths))

    @staticmethod
    def _make_schema_compliant(data_frame: DataFrame, schema) -> DataFrame:
        """adds columns from the schema to the dataframe. (not in-place)"""
        return pl.concat([data_frame, DataFrame(schema=schema)], how="diagonal")

    @staticmethod
    def _split_into_chunks(df: DataFrame, chunksize: int, column="_idx"):
        return (
            ((idx, len(part)), part.drop(column))
            for idx, part in df.with_row_count(column)
            .with_columns(pl.col(column) // chunksize)
            .groupby(column, maintain_order=True)
        )

    def __enter__(self, *args, **kwargs):
        self.__init__(*args, **kwargs)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass
