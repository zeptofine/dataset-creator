import inspect
import os
import warnings
from collections.abc import Collection, Iterable
from datetime import datetime
from pathlib import Path

import polars as pl
from cfg_param_wrapper import CfgDict
from polars import DataFrame, Expr
from tqdm import tqdm

from .base_filters import Column, Comparable, DataFilter, FastComparable
from .custom_toml import TomlCustomCommentDecoder, TomlCustomCommentEncoder


def current_time() -> datetime:
    return datetime.now().replace(microsecond=0)


def _time(_=None) -> datetime:
    return current_time()


class DatasetBuilder:
    def __init__(self, origin: str, config_path: Path) -> None:
        super().__init__()
        self.unready_filters: dict[str, type[DataFilter]] = {}
        self.filters: list[DataFilter] = []
        self.origin: str = origin

        self.config_path = config_path
        self.config = self.generate_config()
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

    def generate_config(self) -> CfgDict:
        dct = CfgDict(
            self.config_path,
            {
                "trim": True,
                "trim_age_limit_secs": 60 * 60 * 24 * 7,
                "trim_check_exists": True,
                "save_interval_secs": 60,
                "chunksize": 100,
                "filepath": "filedb.feather",
            },
            autofill=False,
            start_empty=True,
            save_on_change=False,
            save_mode="toml",
            encoder=TomlCustomCommentEncoder(),
            decoder=TomlCustomCommentDecoder(),
        )
        for name, filter_ in self.unready_filters.items():
            dct[name] = filter_.get_cfg()

        return dct

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
