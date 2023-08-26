import inspect
import os
import warnings
from collections.abc import Collection, Iterable
from datetime import datetime
from pathlib import Path
from typing import TypeVar

import polars as pl
from polars import DataFrame, Expr
from tqdm import tqdm

from .base_rules import Column, Comparable, DataRule, FastComparable


def current_time() -> datetime:
    return datetime.now().replace(microsecond=0)


def _time(_=None) -> datetime:
    return current_time()


T = TypeVar("T")


class DatasetBuilder:
    def __init__(self, origin: str, db_path: Path) -> None:
        super().__init__()
        self.unready_rules: dict[str, type[DataRule]] = {}
        self.rules: list[DataRule] = []
        self.origin: str = origin

        self.filepath: Path = db_path

        self.basic_schema: dict[str, pl.DataType | type] = {"path": str, "checkedtime": pl.Datetime}
        self.build_schema: dict[str, Expr] = {"checkedtime": pl.col("path").apply(_time)}
        self.rule_type_schema: dict[str, pl.DataType | type] = self.basic_schema.copy()
        self.columns: dict[str, Column] = {}

        if self.filepath.exists():
            self.__df: DataFrame = pl.read_ipc(self.filepath, use_pyarrow=True)
        else:
            self.__df: DataFrame = DataFrame(schema=self.basic_schema)

    def add_rules(self, rules: list[type[DataRule] | DataRule]) -> None:
        """Adds rules to the rule list. Rules will be instantiated separately."""

        for rule in rules:
            self.add_rule(rule)

    def add_rule(self, rule: type[DataRule] | DataRule):
        if isinstance(rule, type):
            self.unready_rules[rule.config_keyword] = rule
        elif isinstance(rule, DataRule):
            if rule not in self.rules:
                self.rules.append(rule)

                for column in rule.schema:
                    self.add_schema(column)
        else:
            raise self.NotARuleError(rule)

    def fill_from_config(self, cfg: dict[str, dict], no_warn=False):
        if not len(self.unready_rules):
            raise self.AllAreReadyError()

        for kwd, dct in cfg.items():
            if kwd in self.unready_rules:
                rule = self.unready_rules.pop(kwd)
                sig: inspect.Signature = inspect.signature(rule)

                params = {k: v for k, v in dct.items() if k in sig.parameters and k != "self"}
                self.add_rule(rule(**params))
        if len(self.unready_rules) and not no_warn:
            warnings.warn(f"{self.unready_rules} remain unfilled from config.", stacklevel=2)

    def generate_config(self) -> dict:
        return {name: rule.get_cfg() for name, rule in self.unready_rules.items()}

    def add_schema(self, col: Column):
        self.columns[col.name] = col
        if col.build_method is not None:
            assert col.name not in self.build_schema, f"Column is already in build_schema ({self.build_schema})"
            self.build_schema[col.name] = col.build_method
        if col.name not in self.rule_type_schema:
            self.rule_type_schema[col.name] = col.dtype

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
        assert self.rules, "No rules specified"
        if trim and len(self.__df):
            now: datetime = current_time()
            cond: Expr = now - pl.col("checkedtime") < datetime.fromtimestamp(trim_age_limit)
            if trim_check_exists:
                cond &= pl.col("path").apply(os.path.exists)
            self.__df = self.__df.filter(cond)

        from_full_to_relative: dict[str, Path] = self.get_absolutes(lst)
        if new_paths := set(from_full_to_relative) - set(self.__df.get_column("path")):
            self.__df = pl.concat((self.__df, DataFrame({"path": list(new_paths)})), how="diagonal")

        self.__df = self._comply_to_schema(self.__df, self.rule_type_schema)

        search_cols: set[str] = {*self.build_schema, *self.basic_schema}
        updated_df: DataFrame = self.__df.with_columns(self.rule_type_schema)
        unfinished: DataFrame = updated_df.filter(
            pl.any(pl.col(col).is_null() for col in updated_df.columns if col in search_cols)
        )
        if len(unfinished):

            def trigger_save(save_timer: datetime, collected: list[DataFrame]) -> tuple[datetime, list[DataFrame]]:
                if ((new_time := datetime.now()) - save_timer).total_seconds() > save_interval:
                    data = pl.concat(collected, how="diagonal")
                    self.__df = self.__df.update(data, on="path")
                    self.save_df()
                    return new_time, []
                return save_timer, collected

            def get_nulls_chunks_expr():
                for nulls, group in unfinished.with_columns(self.rule_type_schema).groupby(
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
            self.__df = self.__df.update(pl.concat(collected, how="diagonal"), on="path")
            self.save_df()
        return

    def filter(self, lst, sort_col="path", ignore_missing_columns=False) -> Iterable[Path]:
        assert (
            sort_col in self.__df.columns
        ), f"the column '{sort_col}' is not in the database. Available columns: {self.__df.columns}"
        if len(self.unready_rules):
            warnings.warn(
                f"{len(self.unready_rules)} filters are not initialized and will not be populated", stacklevel=2
            )

        if (missing_requirements := set(self.columns) - set(self.build_schema)) and not ignore_missing_columns:
            raise self.MissingRequirementError(missing_requirements)

        from_full_to_relative: dict[str, Path] = self.get_absolutes(lst)
        paths: set[str] = set(from_full_to_relative.keys())

        vdf: DataFrame = self.__df.filter(pl.col("path").is_in(paths))
        combined = self.combine_exprs(self.rules)
        for rule in combined:
            vdf = rule.compare(vdf, self.__df) if isinstance(rule, Comparable) else vdf.filter(rule)

        return (from_full_to_relative[p] for p in vdf.sort(sort_col).get_column("path"))

    @staticmethod
    def combine_exprs(rules: Iterable[DataRule]) -> list[Expr | bool | Comparable]:
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

    @property
    def df(self):
        return self.__df

    class NotARuleError(TypeError):
        def __init__(self, obj: object):
            super().__init__(f"{obj} is not a valid rule")

    class AllAreReadyError(KeyError):
        def __init__(self):
            super().__init__("Unready rules are empty")

    class MissingRequirementError(KeyError):
        def __init__(self, reqs: Iterable[str]):
            super().__init__(f"Possibly missing columns: {reqs}")

    def __enter__(self, *args, **kwargs):
        self.__init__(*args, **kwargs)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass
