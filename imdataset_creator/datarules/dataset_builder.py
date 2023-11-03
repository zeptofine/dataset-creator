import inspect
import os
import textwrap
import warnings
from collections.abc import Collection, Iterable
from datetime import datetime
from pathlib import Path
from typing import Generator, Literal, TypeVar, overload

import polars as pl
from polars import DataFrame, Expr, LazyFrame
from polars.type_aliases import SchemaDefinition

from .base_rules import (
    DataFrameMatcher,
    DataTypeSchema,
    ExprDict,
    ExprMatcher,
    Producer,
    ProducerSchema,
    ProducerSet,
    Rule,
)


def indent(t):
    return textwrap.indent(t, "    ")


def current_time() -> datetime:
    return datetime.now().replace(microsecond=0)


T = TypeVar("T")


def chunk_split(
    df: DataFrame,
    chunksize: int,
    col_name: str = "_idx",
) -> Generator[DataFrame, None, None]:
    """
    Splits a dataframe into chunks based on index.

    Args:
        df (DataFrame): the dataframe to split
        chunksize (int): the size of each resulting chunk
        col_name (str, optional): the name of the temporary chunk. Defaults to "_idx".

    Yields:
        Generator[DataFrame, None, None]: a chunk
    """
    return (
        part.drop(col_name)
        for _, part in df.with_row_count(col_name)
        .with_columns(pl.col(col_name) // chunksize)
        .groupby(col_name, maintain_order=True)
    )


def combine_matchers(
    matchers: Iterable[DataFrameMatcher | ExprMatcher],
) -> Generator[Expr | DataFrameMatcher, None, None]:
    """this combines expressions from different matchers to compressed expressions.
    DataRules that are `ExprMatcher`s can be combined, but `DataFrameMatcher`s cannot. They will be copied to the list.
    """
    combination: Expr | bool | None = None
    for matcher in matchers:
        if isinstance(matcher, DataFrameMatcher):
            if combination is not None:
                yield combination
                combination = None
            yield matcher
        elif isinstance(matcher, ExprMatcher):
            combination = combination & matcher() if combination is not None else matcher()
    if combination is not None:
        yield combination


def blacklist_schema(schema: ProducerSchema, blacklist: Collection) -> ProducerSchema:
    return [out for dct in schema if (out := {k: v for k, v in dct.items() if k not in blacklist})]


class DatasetBuilder:
    def __init__(self, db_path: Path) -> None:
        super().__init__()
        self.producers: ProducerSet = ProducerSet()
        self.unready_rules: dict[str, type[Rule]] = {}
        self.rules: list[Rule] = []

        self.filepath: Path = db_path

        self.basic_schema: DataTypeSchema = {"path": str}

        self.__df: DataFrame
        if self.filepath.exists():
            self.__df = pl.read_ipc(self.filepath, use_pyarrow=True, memory_map=False)
        else:
            self.__df = DataFrame(schema=self.basic_schema)

    def add_rules(self, *rules: type[Rule] | Rule) -> None:
        """Adds rules to the rule list. Rules can be instantiated separately."""

        for rule in rules:
            self.add_rule(rule)

    def add_rule(self, rule: type[Rule] | Rule):
        if isinstance(rule, type):
            self.unready_rules[rule.cfg_kwd()] = rule
        elif isinstance(rule, Rule):
            if rule not in self.rules:
                self.rules.append(rule)
        else:
            raise self.NotARuleError(rule)

    @property
    def type_schema(self) -> DataTypeSchema:
        schema: DataTypeSchema = self.basic_schema.copy()
        schema.update(self.producers.type_schema)
        return schema

    def add_producer(self, producer: Producer):
        self.producers.add(producer)

    def add_producers(self, *producers: Producer):
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
            warnings.warn(f"{self.unready_rules} remain unfilled from config.", stacklevel=2)

    def generate_config(self) -> dict:
        return {name: rule.get_cfg() for name, rule in self.unready_rules.items()}

    def add_new_paths(self, pths: set[str]) -> bool:
        """
        - Adds paths to the df.

        Parameters
        ----------
        pths : set[str]
            The paths to add to the dataframe.

        Returns
        -------
        bool
            whether any new paths were added to the dataframe
        """
        if new_paths := pths - set(self.__df.get_column("path")):
            self.__df = pl.concat((self.__df, DataFrame({"path": list(new_paths)})), how="diagonal")
            return True
        return False

    def get_unfinished_producers(self) -> ProducerSet:
        """
        gets producers that do not have their column requirements fulfilled.

        Returns
        -------
        set[Producer]
            unfinished producers.
        """
        return ProducerSet(
            producer
            for producer in self.producers
            if not set(producer.produces) - set(self.__df.columns)
            and not self.__df.filter(
                pl.all_horizontal(pl.col(col).is_null() for col in producer.produces),
            ).is_empty()
        )

    def remove_finished_producers(self) -> ProducerSet:
        """
        Takes the completed producers out of the set

        Returns
        -------
        ProducerSet
            finished producers
        """
        new: ProducerSet = self.get_unfinished_producers()
        old: ProducerSet = self.producers
        self.producers = new
        return ProducerSet(old - new)

    @overload
    def unfinished_by_col(self, df: DataFrame, cols: Iterable[str] | None = None) -> DataFrame:
        ...

    @overload
    def unfinished_by_col(self, df: LazyFrame, cols: Iterable[str] | None = None) -> LazyFrame:
        ...

    def unfinished_by_col(self, df: DataFrame | LazyFrame, cols: Iterable[str] | None = None) -> DataFrame | LazyFrame:
        if cols is None:
            cols = set(self.type_schema) & set(df.columns)
        return df.filter(pl.any_horizontal(pl.col(col).is_null() for col in cols))

    def split_files_via_nulls(
        self,
        df: DataFrame,
        schema: ProducerSchema | None = None,
    ) -> Generator[tuple[ProducerSchema, DataFrame], None, None]:
        """
        groups a df by nulls, and gets a schema based on the nulls

        Parameters
        ----------
        df : DataFrame
            the dataframe to split
        schema : ProducerSchema | None, optional
            the schema to combine with the groups, by default None

        Yields
        ------
        Generator[tuple[ProducerSchema, DataFrame], None, None]
            each group is given separately
        """
        if schema is None:
            schema = self.get_unfinished_producers().schema
        # Split the data into groups based on null values in columns
        for nulls, group in df.groupby(*(pl.col(col).is_null() for col in df.columns)):
            truth_table: set[str] = {
                col
                for col, truth in zip(  # type: ignore
                    df.columns,
                    *nulls if hasattr(nulls, "__next__") else (nulls,),
                )
                if not truth
            }
            yield (
                blacklist_schema(schema, truth_table),
                group,
            )

    def populate_chunks(
        self,
        chunks: Iterable[DataFrame],
        schemas: ProducerSchema,
        db_schema: DataTypeSchema | None = None,
    ) -> Generator[DataFrame, None, None]:
        if db_schema is None:
            db_schema = self.type_schema
        chunk: DataFrame
        for chunk in chunks:
            # current_paths = list(chunk.get_column("path"))  # used for debugging
            for schema in schemas:
                chunk = chunk.with_columns(**schema)
            chunk = chunk.select(db_schema)
            yield chunk

    def df_with_types(self, types: DataTypeSchema | None = None):
        if types is None:
            types = self.type_schema
        return self.comply_to_schema(types).with_columns(types)

    def get_unfinished(self) -> LazyFrame:
        # check if producers are completely finished
        type_schema: DataTypeSchema = self.type_schema
        self.comply_to_schema(type_schema, in_place=True)
        return self.unfinished_by_col(self.__df.lazy().with_columns(type_schema))

    def get_unfinished_existing(self) -> LazyFrame:
        return self.get_unfinished().filter(pl.col("path").apply(os.path.exists))

    def filter(self, lst) -> DataFrame:  # noqa: A003
        if len(self.unready_rules):
            warnings.warn(
                f"{len(self.unready_rules)} filters are not initialized and will not be populated",
                stacklevel=2,
            )

        vdf: DataFrame = self.__df.filter(pl.col("path").is_in(lst))
        for matcher in combine_matchers(rule.matcher for rule in self.rules):
            if not len(vdf):
                break
            vdf = matcher(vdf, self.__df) if isinstance(matcher, DataFrameMatcher) else vdf.filter(matcher)
        return vdf

    def save_df(self, pth: str | Path | None = None) -> None:
        """saves the dataframe to self.filepath"""
        self.__df.write_ipc(pth or self.filepath)

    def update(self, df: DataFrame, on="path", how: Literal["left", "inner", "outer"] = "left"):
        self.__df = self.__df.update(df, on=on, how=how)

    def trigger_save_via_time(
        self, save_timer: datetime, collected: list[DataFrame], interval=60
    ) -> tuple[datetime, list[DataFrame]]:
        if ((new_time := datetime.now()) - save_timer).total_seconds() > interval:
            data: DataFrame = pl.concat(collected, how="diagonal")
            self.update(data)
            self.save_df()
            return new_time, []
        return save_timer, collected

    @property
    def df(self) -> DataFrame:
        return self.__df

    class NotARuleError(TypeError):
        def __init__(self, obj: object):
            super().__init__(f"{obj} is not a valid rule")

    class MissingRequirementError(KeyError):
        def __init__(self, reqs: Iterable[str]):
            super().__init__(f"Possibly missing columns: {reqs}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass

    def __repr__(self) -> str:
        attrlist: list[str] = [
            f"{key}={val!r}" for key, val in vars(self).items() if all(k not in key for k in ("__",))
        ]
        return f"{self.__class__.__name__}({', '.join(attrlist)})"

    @overload
    def comply_to_schema(self, schema: SchemaDefinition) -> DataFrame:
        ...

    @overload
    def comply_to_schema(self, schema: SchemaDefinition, in_place=False) -> DataFrame:
        ...

    @overload
    def comply_to_schema(self, schema: SchemaDefinition, in_place=True) -> None:
        ...

    def comply_to_schema(self, schema: SchemaDefinition, in_place: bool = False) -> DataFrame | None:
        new_df: DataFrame = pl.concat((self.__df, DataFrame(schema=schema)), how="diagonal")
        if in_place:
            self.__df = new_df
            return None
        return new_df
