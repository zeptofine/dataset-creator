import os
from collections.abc import Collection, Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
from cfg_param_wrapper import CfgDict
from polars import DataFrame, Expr, PolarsDataType
from rich import print as rprint
from tqdm import tqdm

from util.print_funcs import byte_format

from .base_filters import Comparable, DataFilter, FastComparable


def current_time() -> datetime:
    return datetime.now().replace(microsecond=0)


def _time(_=None) -> datetime:
    return current_time()


class DatasetBuilder:
    def __init__(self, origin: str) -> None:
        super().__init__()
        self.filters: list[DataFilter] = []
        self.origin: str = origin

        self.config = CfgDict(
            "database_config.toml",
            {
                "trim": True,
                "trim_age_limit_secs": 60 * 60 * 24 * 7,
                "trim_check_exists": True,
                "save_interval_secs": 60,
                "chunksize": 100,
                "filepath": "filedb.feather",
            },
            autofill=True,
            save_mode="toml",
        )
        self.filepath: str = self.config["filepath"]
        self.trim: bool = self.config["trim"]
        self.time_threshold: datetime = datetime.fromtimestamp(self.config["trim_age_limit_secs"])
        self.check_exists: bool = self.config["trim_check_exists"]

        self.basic_schema = {"path": str, "checkedtime": pl.Datetime}
        self.build_schema: dict[str, Expr] = {"checkedtime": pl.col("path").apply(_time)}
        if os.path.exists(self.filepath):
            before = datetime.now()
            print("Reading database...")
            self.df: DataFrame = pl.read_ipc(self.config["filepath"], use_pyarrow=True)
            print(f"Finished reading in {datetime.now() - before}")
        else:
            self.df: DataFrame = DataFrame(schema=self.basic_schema)

    def add_filters(self, *filters: DataFilter) -> None:
        """Adds filters to the filter list."""
        for filter_ in filters:
            self.filters.append(filter_)
            if filter_.build_schema:
                if any(col not in self.build_schema for col in filter_.build_schema):
                    self.add_schema_from_filter(filter_)

    def add_schema_from_filter(self, filter_: DataFilter, overwrite=False):
        assert filter_.build_schema, f"{filter_} has no build_schema"
        if not overwrite:
            assert all(
                key not in self.build_schema for key in filter_.build_schema.keys()
            ), f"Schema is already in build_schema: {self.build_schema}"
        self.build_schema.update(filter_.build_schema)

    def populate_df(self, lst: Iterable[Path]):
        assert self.filters, "No filters specified"
        if self.trim and len(self.df):
            now: datetime = current_time()
            original_size: int = len(self.df)

            cond: Expr = now - pl.col("checkedtime") < self.time_threshold
            if self.check_exists:
                cond &= pl.col("path").apply(os.path.exists)

            self.df = self.df.filter(cond)
            if diff := original_size - len(self.df):
                print(f"Removed {diff} images")

        from_full_to_relative: dict[str, Path] = self.absolute_dict(lst)
        abs_paths: set[str] = set(from_full_to_relative.keys())

        # add new paths to the dataframe with missing data
        existing_paths = set(self.df.select(pl.col("path")).to_series())
        new_paths: list[str] = [path for path in abs_paths if path not in existing_paths]
        if new_paths:
            self.df = pl.concat(
                [self.df, DataFrame({"path": new_paths})],
                how="diagonal",
            )
        column_schema: dict[str, PolarsDataType | type] = self.basic_schema.copy()
        build_schema: dict[str, Expr] = {"checkedtime": pl.col("path").apply(_time)}
        for filter_ in self.filters:
            filter_.filedict = from_full_to_relative
            if filter_.column_schema:
                column_schema.update(filter_.column_schema)
            if filter_.build_schema:
                build_schema.update(filter_.build_schema)

        self.df = self._make_schema_compliant(self.df, column_schema)
        updated_df: DataFrame = self.df.with_columns(column_schema)
        unfinished: DataFrame = updated_df.filter(pl.any(pl.col(col).is_null() for col in updated_df.columns))
        if len(unfinished):
            old_db_size: str = byte_format(self.get_db_disk_size())
            with tqdm(desc="Gathering file info...", total=len(unfinished)) as t:
                chunksize: int = self.config["chunksize"]
                save_timer = datetime.now()
                collected_data: DataFrame = DataFrame(schema=column_schema)
                for group in (
                    unfinished.with_row_count("idx").with_columns(pl.col("idx") // chunksize).partition_by("idx")
                ):
                    new_data: DataFrame = self.fill_nulls(group, build_schema).drop("idx").select(column_schema)
                    collected_data.vstack(new_data, in_place=True)
                    t.update(len(group))
                    if ((new_time := datetime.now()) - save_timer).total_seconds() > self.config["save_interval_secs"]:
                        self.df = self.df.update(collected_data, on="path")
                        self.save_df()
                        t.set_postfix_str(f"Autosaved at {current_time()}")
                        collected_data = collected_data.clear()
                        save_timer = new_time

            self.df = self.df.update(collected_data, on="path").rechunk()
            self.save_df()
            rprint(f"old DB size: [bold red]{old_db_size}[/bold red]")
            rprint(f"new DB size: [bold yellow]{byte_format(self.get_db_disk_size())}[/bold yellow]")
        return

    @staticmethod
    def fill_nulls(data_frame: DataFrame, build_expr: dict[str, Expr]) -> DataFrame:
        return (
            data_frame.with_row_count("i")
            .groupby("i")
            .apply(lambda item: DatasetBuilder.apply_only_on_null(item, build_expr))
            .drop("i")
        )

    @staticmethod
    def apply_only_on_null(item: DataFrame, build_expr: dict[str, Expr]):
        # only use expressions that are valid
        dct_: dict[str, Any] = item.row(0, named=True)
        if all(value is None for value in dct_.values()):
            return item.with_columns(**build_expr)
        return item.with_columns(**{col: expr for col, expr in build_expr.items() if dct_[col] is None})

    def filter(self, lst, sort_col="path") -> list[Path]:
        assert (
            sort_col in self.df.columns
        ), f"the column '{sort_col}' is not in the database. Available columns: {self.df.columns}"

        from_full_to_relative: dict[str, Path] = self.absolute_dict(lst)
        paths: set[str] = set(from_full_to_relative.keys())
        with tqdm(self.filters, "Running full filters...") as t:
            vdf: DataFrame = self.df.filter(pl.col("path").is_in(paths)).rechunk()
            count = 0
            print(f"Original size: {len(vdf)}")
            for dfilter in self.filters:
                if len(vdf) == 0:
                    break
                if isinstance(dfilter, FastComparable):
                    vdf = vdf.filter(dfilter.fast_comp())
                elif isinstance(dfilter, Comparable):
                    vdf = vdf.filter(
                        pl.col("path").is_in(
                            dfilter.compare(
                                set(vdf.select(pl.col("path")).to_series()),
                                self.df.select(pl.col("path"), *[pl.col(col) for col in dfilter.column_schema]),
                            )
                        )
                    )
                print(f"{dfilter}: {len(vdf)}")
                t.update(count + 1)
                count = 0
            t.update(count)
        return [from_full_to_relative[p] for p in vdf.sort(sort_col).select(pl.col("path")).to_series()]

    def get_db_disk_size(self) -> int:
        """gets the database size on disk."""
        if not os.path.exists(self.config["filepath"]):
            return 0
        return os.stat(self.config["filepath"]).st_size

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

    def __enter__(self, *args, **kwargs):
        self.__init__(*args, **kwargs)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass
