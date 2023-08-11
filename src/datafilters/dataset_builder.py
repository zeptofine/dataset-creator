import os
from collections.abc import Collection, Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
from cfg_param_wrapper import CfgDict
from polars import DataFrame, Expr
from tqdm import tqdm


from .base_filters import Comparable, DataFilter, FastComparable, Column
from .custom_toml import TomlCustomCommentDecoder, TomlCustomCommentEncoder


def current_time() -> datetime:
    return datetime.now().replace(microsecond=0)


def _time(_=None) -> datetime:
    return current_time()


class DatasetBuilder:
    def __init__(self, origin: str, config_path: Path) -> None:
        super().__init__()
        self.filters: list[DataFilter] = []
        self.origin: str = origin

        self.config = CfgDict(
            config_path,
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
            encoder=TomlCustomCommentEncoder(),
            decoder=TomlCustomCommentDecoder(),
        )
        self.filepath: str = self.config["filepath"]
        self.trim: bool = self.config["trim"]
        self.time_threshold: datetime = datetime.fromtimestamp(self.config["trim_age_limit_secs"])
        self.check_exists: bool = self.config["trim_check_exists"]

        self.basic_schema = {"path": str, "checkedtime": pl.Datetime}
        self.column_schema = self.basic_schema.copy()
        self.build_schema: dict[str, Expr] = {"checkedtime": pl.col("path").apply(_time)}
        if os.path.exists(self.filepath):
            self.df: DataFrame = pl.read_ipc(self.config["filepath"], use_pyarrow=True)
        else:
            self.df: DataFrame = DataFrame(schema=self.basic_schema)

    def add_filters(self, *filters: DataFilter) -> None:
        """Adds filters to the filter list."""
        new_confs = False
        for filter_ in filters:
            filter_conf: tuple[str | None, dict[str, Any]] = filter_.get_config()
            if filter_conf[0] not in (*self.config, None):
                self.config.update({filter_conf[0]: filter_conf[1]})
                print(f"New filter added to config: {filter_conf[0]}. check database_config.toml to edit.")
                new_confs = True
            if filter_conf[0] is not None:
                filter_.populate_from_cfg(self.config[filter_conf[0]])

                if self.config[filter_conf[0]].get("enabled", False):
                    self.filters.append(filter_)
                    filter_.enable()
                    for column in filter_.schema:
                        if column.build_method is not None:
                            if column.name not in self.build_schema:
                                self.add_schema(column)

        if new_confs:
            self.config.save()

    def add_schema(self, col: Column, overwrite=False):
        if not overwrite:
            assert col.name not in self.build_schema, f"Column is already in build_schema ({self.build_schema})"
        if col.build_method is not None:
            self.build_schema[col.name] = col.build_method
        if col.name not in self.column_schema:
            self.column_schema[col.name] = col.dtype

    def populate_df(self, lst: Iterable[Path]):
        assert self.filters, "No filters specified"
        if self.trim and len(self.df):
            now: datetime = current_time()
            cond: Expr = now - pl.col("checkedtime") < self.time_threshold
            if self.check_exists:
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

        self.df = self._make_schema_compliant(self.df, self.column_schema)
        updated_df: DataFrame = self.df.with_columns(self.column_schema)
        search_cols: set[str] = {*self.build_schema, *self.basic_schema}
        unfinished: DataFrame = updated_df.filter(
            pl.any(pl.col(col).is_null() for col in updated_df.columns if col in search_cols)
        )
        if len(unfinished):
            with tqdm(desc="Gathering file info...", total=len(unfinished)) as t:
                chunksize: int = self.config["chunksize"]
                save_timer = datetime.now()
                collected_data: DataFrame = DataFrame(schema=self.column_schema)
                for nulls, group in (
                    unfinished.with_columns(self.column_schema)
                    .with_row_count("idx")
                    .with_columns(pl.col("idx") // chunksize)
                    .groupby("idx", *(pl.col(col).is_not_null() for col in self.build_schema))
                ):
                    t.set_postfix_str(str(nulls))
                    new_data = group.drop("idx").with_columns(
                        **{
                            col: expr
                            for truth, (col, expr) in zip(nulls[1:], self.build_schema.items())  # type: ignore
                            if not truth
                        }
                    )
                    collected_data = pl.concat([collected_data, new_data], how="diagonal")
                    t.update(len(group))
                    if ((new_time := datetime.now()) - save_timer).total_seconds() > self.config["save_interval_secs"]:
                        self.df = self.df.update(collected_data, on="path")
                        self.save_df()
                        t.set_postfix_str(f"Autosaved at {current_time()}")
                        collected_data = collected_data.clear()
                        save_timer = new_time

            self.df = self.df.update(collected_data, on="path").rechunk()
            self.save_df()
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
                                self.df.select(pl.col("path"), *[pl.col(col.name) for col in dfilter.schema]),
                            )
                        )
                    )
                t.update(count + 1)
                count = 0
            t.update(count)
        return [from_full_to_relative[p] for p in vdf.sort(sort_col).select(pl.col("path")).to_series()]

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
