import inspect
import warnings
from collections.abc import Collection, Iterable
from datetime import datetime
from pathlib import Path
from typing import Generator, TypeVar

import polars as pl
from polars import DataFrame, Expr
from rich import print as rprint
from tqdm import tqdm

from .base_rules import Comparable, FastComparable, Producer, Rule


def current_time() -> datetime:
    return datetime.now().replace(microsecond=0)


def _time(_=None) -> datetime:
    return current_time()


T = TypeVar("T")


class DatasetBuilder:
    def __init__(self, origin: str, db_path: Path) -> None:
        super().__init__()
        self.producers: set[Producer] = set()
        self.unready_rules: dict[str, type[Rule]] = {}
        self.rules: list[Rule] = []
        self.origin: str = origin

        self.filepath: Path = db_path

        self.basic_schema: dict[str, pl.DataType | type] = {"path": str}

        if self.filepath.exists():
            self.__df: DataFrame = pl.read_ipc(self.filepath, use_pyarrow=True)
        else:
            self.__df: DataFrame = DataFrame(schema=self.basic_schema)

    def add_rules(self, rules: Iterable[type[Rule] | Rule]) -> None:
        """Adds rules to the rule list. Rules will be instantiated separately."""

        for rule in rules:
            self.add_rule(rule)

    def add_rule(self, rule: type[Rule] | Rule):
        if isinstance(rule, type):
            self.unready_rules[rule.config_keyword] = rule
        elif isinstance(rule, Rule):
            if rule not in self.rules:
                self.rules.append(rule)
        else:
            raise self.NotARuleError(rule)

    def get_type_schema(self) -> dict[str, pl.DataType | type]:
        schema: dict[str, pl.DataType | type] = self.basic_schema.copy()
        schema.update({col: dtype for producer in self.producers for col, dtype in producer.produces.items()})
        return schema

    def add_producer(self, producer: Producer):
        self.producers.add(producer)

    def add_producers(self, producers: Iterable[Producer]):
        for producer in producers:
            self.add_producer(producer)

    def fill_from_config(self, cfg: dict[str, dict], no_warn=False):
        if not len(self.unready_rules):
            return

        for kwd, dct in cfg.items():
            if kwd in self.unready_rules:
                rule = self.unready_rules.pop(kwd)
                sig: inspect.Signature = inspect.signature(rule)

                params = {k: v for k, v in dct.items() if k in sig.parameters and k != "self"}
                self.add_rule(rule(**params))
        if len(self.unready_rules) and not no_warn:
            warnings.warn(f"{self.unready_rules} remain unfilled from config.", stacklevel=2)  # type: ignore

    def generate_config(self) -> dict:
        return {name: rule.get_cfg() for name, rule in self.unready_rules.items()}

    def populate_df(
        self,
        lst: Iterable[Path],
        use_tqdm=True,
        save_interval: int = 60,
        save=True,
        chunksize: int = 100,
    ):
        assert self.producers, "No producers specified"

        # add new paths to the df
        if new_paths := set(self.get_absolutes(lst)) - set(self.__df.get_column("path")):
            self.__df = pl.concat((self.__df, DataFrame({"path": list(new_paths)})), how="diagonal")

        # check if producers are completely finished
        potential_producers: set[Producer] = self.producers.copy()
        for producer in self.producers:
            if set(producer.produces) - set(self.__df.columns):
                continue
            if self.__df.filter(pl.all(pl.col(col).is_null() for col in producer.produces)).is_empty():
                potential_producers.remove(producer)

        #
        type_schema: dict[str, pl.DataType | type] = self.get_type_schema()
        build_schemas: list[dict[str, Expr | bool]] = Producer.build_producer_schema(potential_producers)
        self.__df = self._comply_to_schema(self.__df, type_schema)
        updated_df: DataFrame = self.__df.with_columns(type_schema)
        search_cols: set[str] = {*type_schema}

        unfinished: DataFrame = updated_df.filter(
            pl.any(pl.col(col).is_null() for col in updated_df.columns if col in search_cols)
        )

        if len(unfinished):

            def trigger_save(save_timer: datetime, collected: list[DataFrame]) -> tuple[datetime, list[DataFrame]]:
                if save and ((new_time := datetime.now()) - save_timer).total_seconds() > save_interval:
                    self.__df = self.__df.update(pl.concat(collected, how="diagonal"), on="path")
                    if save:
                        self.save_df()
                    return new_time, []
                return save_timer, collected

            save_timer: datetime = datetime.now()
            collected: list[DataFrame] = []

            splitted = (
                (
                    dict(zip(unfinished.columns, *nulls if hasattr(nulls, "__next__") else (nulls,))),  # type: ignore
                    group,
                )
                for nulls, group in unfinished.groupby(*(pl.col(col).is_null() for col in unfinished.columns))
            )

            splitted_with_schema: Generator[tuple[list[dict[str, Expr | bool]], DataFrame], None, None] = (
                (
                    self.blacklist_schema(build_schemas, {col for col, truth in truth_table.items() if not truth}),
                    df,
                )
                for truth_table, df in splitted
            )

            if not use_tqdm:
                for schemas, df in splitted_with_schema:
                    for _, chunk in self._split_into_chunks(df, chunksize=chunksize):
                        for schema in schemas:
                            chunk = chunk.with_columns(**schema)
                        collected.append(chunk.select(type_schema))
                        save_timer, collected = trigger_save(save_timer, collected)
            else:
                with (
                    tqdm(desc="Gathering...", unit="file", total=len(unfinished)) as total_t,
                    tqdm(unit="chunk", total=len(unfinished) // chunksize) as sub_t,
                ):
                    for schemas, df in splitted_with_schema:
                        chunks = list(self._split_into_chunks(df, chunksize=chunksize))
                        sub_t.total = len(chunks)
                        sub_t.update(-sub_t.n)
                        for (idx, size), chunk in chunks:
                            for schema in schemas:
                                chunk = chunk.with_columns(**schema)
                            chunk = chunk.select(type_schema)
                            collected.append(chunk)
                            save_timer, collected = trigger_save(save_timer, collected)

                            sub_t.set_postfix_str(str(idx))
                            sub_t.update()
                            total_t.update(size)

            # This breaks with datatypes like Array(3, pl.UInt32). Not sure why.
            # `pyo3_runtime.PanicException: implementation error, cannot get ref Array(Null, 0) from Array(UInt32, 3)`
            self.__df = self.__df.update(pl.concat(collected, how="diagonal"), on="path")
            if save:
                self.save_df()
        return

    def filter(self, lst, sort_col="path") -> Iterable[Path]:
        assert sort_col in self.__df.columns, f"'{sort_col}' is not in {self.__df.columns}"
        if len(self.unready_rules):
            warnings.warn(
                f"{len(self.unready_rules)} filters are not initialized and will not be populated", stacklevel=2
            )

        fulltorelativedict: dict[str, Path] = self.get_absolutes(lst)

        vdf: DataFrame = self.__df.filter(pl.col("path").is_in(fulltorelativedict.keys()))
        combined = self.combine_exprs(self.rules)
        for rule in combined:
            vdf = rule.compare(vdf, self.__df) if isinstance(rule, Comparable) else vdf.filter(rule)

        return (fulltorelativedict[p] for p in vdf.sort(sort_col).get_column("path"))

    @staticmethod
    def combine_exprs(rules: Iterable[Rule]) -> list[Expr | bool | Comparable]:
        """this combines expressions from different objects to a list of compressed expressions.
        DataRules that are FastComparable can be combined, but Comparables cannot. They will be copied to the list.
        """
        combinations: list[Expr | bool | Comparable] = []
        combination: Expr | bool | None = None
        for rule in rules:
            if isinstance(rule, FastComparable):
                combination = combination & rule.fast_comp() if combination is not None else rule.fast_comp()
            elif isinstance(rule, Comparable):
                if combination is not None:
                    combinations.append(combination)
                    combination = None
                combinations.append(rule)
        if combination is not None:
            combinations.append(combination)

        return combinations

    def save_df(self) -> None:
        """saves the dataframe to self.filepath"""
        self.__df.write_ipc(self.filepath)

    def get_absolutes(self, lst: Iterable[Path]) -> dict[str, Path]:
        return {(str((self.origin / pth).resolve())): pth for pth in lst}  # type: ignore

    def get_path_data(self, pths: Collection[str]):
        return self.__df.filter(pl.col("path").is_in(pths))

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

    @staticmethod
    def blacklist_schema(schema: list[dict[str, Expr | bool]], blacklist: Iterable) -> list[dict[str, Expr | bool]]:
        return [out for dct in schema if (out := {k: v for k, v in dct.items() if k not in blacklist})]

    @property
    def df(self):
        return self.__df

    class NotARuleError(TypeError):
        def __init__(self, obj: object):
            super().__init__(f"{obj} is not a valid rule")

    class MissingRequirementError(KeyError):
        def __init__(self, reqs: Iterable[str]):
            super().__init__(f"Possibly missing columns: {reqs}")

    def __enter__(self, *args, **kwargs):
        self.__init__(*args, **kwargs)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass
