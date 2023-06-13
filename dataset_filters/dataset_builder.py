import os
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

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
    def __init__(self, origin: str, processes=1) -> None:
        super().__init__()
        self.filters: list[DataFilter] = []
        self.power: int = processes
        self.origin: str = origin

        self.config = CfgDict(
            "database_config.toml",
            {
                "trim": True,
                "trim_age_limit_secs": 60 * 60 * 24 * 7,
                "trim_check_exists": True,
                "save_interval": 500,
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
            print("Reading database...")
            self.df = pl.read_ipc(self.config["filepath"], use_pyarrow=True)
            print("Finished.")
        else:
            self.df = DataFrame(schema=self.basic_schema)

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

    def add_optional_from_filter(self, filter_: DataFilter):
        assert filter_.build_schema, f"{filter_} has no build_schema"
        self.build_schema.update(filter_.build_schema)

    def populate_df(self, lst: Iterable[Path]):
        if self.trim and len(self.df):
            print("Attempting to trim db...")
            now: datetime = current_time()
            original_size: int = len(self.df)

            cond: Expr = now - pl.col("checkedtime") < self.time_threshold
            if self.check_exists:
                cond &= pl.col("path").apply(os.path.exists)

            self.df = self.df.filter(cond).rechunk()
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

        modified_schema: dict[str, PolarsDataType | type] = dict(self.df.schema).copy()
        for filter_ in self.filters:
            filter_.filedict = from_full_to_relative
            modified_schema.update(
                {
                    schema: value for schema, value in filter_.column_schema.items() if schema not in self.df.schema
                }  # type: ignore
            )

        full_build_expr: dict[str, Expr] = {
            col: pl.when(pl.col(col).is_null()).then(expr).otherwise(pl.col(col))
            for col, expr in self.build_schema.items()
            if col in self.df.columns or col in modified_schema
        }

        # get paths with missing data
        self.df: DataFrame = DatasetBuilder._make_schema_compliant(self.df, modified_schema)

        unfinished: DataFrame = self.df.filter(pl.any(pl.col(col).is_null() for col in self.df.columns))

        if len(unfinished):
            try:
                # gather new data and add it to the dataframe
                old_db_size: str = byte_format(self.get_db_disk_size())
                with tqdm(desc="Gathering file info...", total=len(unfinished)) as t:
                    chunksize: int = self.config["chunksize"]
                    save_timer = 0
                    collected_data = DataFrame(schema=modified_schema)  # type: ignore
                    for group in (
                        unfinished.with_row_count("idx").with_columns(pl.col("idx") // chunksize).partition_by("idx")
                    ):
                        group.drop_in_place("idx")
                        new_data: DataFrame = group.with_columns(**full_build_expr)
                        collected_data.vstack(new_data, in_place=True)
                        t.update(len(group))

                        save_timer += len(group)
                        if save_timer > self.config["save_interval"]:
                            self.df = self.df.update(collected_data, on="path")
                            self.save_df()
                            t.set_postfix_str(f"Autosaved at {current_time()}")
                            collected_data: DataFrame = collected_data.clear()
                            save_timer = 0

                    self.df = self.df.update(collected_data, on="path").rechunk()
                    self.save_df()
                    rprint(f"old DB size: [bold red]{old_db_size}[/bold red]")
                    rprint(f"new DB size: [bold yellow]{byte_format(self.get_db_disk_size())}[/bold yellow]")
            except KeyboardInterrupt as exc:
                print("KeyboardInterrupt detected! attempting to save dataframe...")
                self.save_df()
                print("Saved.")
                raise exc

        return

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

    @staticmethod
    def _make_schema_compliant(data_frame: DataFrame, schema) -> DataFrame:
        """adds columns from the schema to the dataframe. (not in-place)"""
        return pl.concat([data_frame, DataFrame(schema=schema)], how="diagonal")

    def __enter__(self, *args, **kwargs):
        self.__init__(*args, **kwargs)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass
