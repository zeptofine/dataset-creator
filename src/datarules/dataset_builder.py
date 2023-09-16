import inspect
import warnings
from collections.abc import Collection, Iterable
from datetime import datetime
from pathlib import Path
from typing import Generator, Literal, TypeVar, overload

import polars as pl
from polars import DataFrame, Expr
from polars.type_aliases import SchemaDefinition

from .base_rules import Comparable, ExprDict, FastComparable, Producer, Rule, combine_schema


def current_time() -> datetime:
    return datetime.now().replace(microsecond=0)


T = TypeVar("T")

DataTypeSchema = dict[str, pl.DataType | type]


class DatasetBuilder:
    def __init__(self, db_path: Path) -> None:
        super().__init__()
        self.producers: set[Producer] = set()
        self.unready_rules: dict[str, type[Rule]] = {}
        self.rules: list[Rule] = []

        self.filepath: Path = db_path

        self.basic_schema: DataTypeSchema = {"path": str}

        self.__df: DataFrame
        if self.filepath.exists():
            self.__df = pl.read_ipc(self.filepath, use_pyarrow=True, memory_map=False)
        else:
            self.__df = DataFrame(schema=self.basic_schema)

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

    @property
    def type_schema(self) -> DataTypeSchema:
        schema: DataTypeSchema = self.basic_schema.copy()
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
            warnings.warn(f"{self.unready_rules} remain unfilled from config.", stacklevel=2)

    def generate_config(self) -> dict:
        return {name: rule.get_cfg() for name, rule in self.unready_rules.items()}

    def add_new_paths(self, pths: set[str]) -> bool:
        """
        - Adds paths to the df.

        Parameters
        ----------
        pths : set[os.PathLike]
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

    def get_unfinished_producers(self) -> set[Producer]:
        """
        gets producers that do not have their column requirements fulfilled.

        Returns
        -------
        set[Producer]
            unfinished producers.
        """
        return {
            producer
            for producer in self.producers
            if not set(producer.produces) - set(self.__df.columns)
            and not self.__df.filter(pl.all_horizontal(pl.col(col).is_null() for col in producer.produces)).is_empty()
        }

    def unfinished_by_col(self, df: DataFrame, cols: Iterable[str] | None = None) -> DataFrame:
        if cols is None:
            cols = {*self.type_schema} & set(df.columns)
        return df.filter(pl.any_horizontal(pl.col(col).is_null() for col in cols))

    def split_files_via_nulls(
        self,
        df: DataFrame,
        schema: list[ExprDict] | None = None,
    ) -> Generator[tuple[list[ExprDict], DataFrame], None, None]:
        """
        groups a df by nulls, and gets a schema based on the nulls

        Parameters
        ----------
        df : DataFrame
            the dataframe to split
        schema : list[ExprDict] | None, optional
            the schema to combine with the groups, by default None

        Yields
        ------
        Generator[tuple[list[ExprDict], DataFrame], None, None]
            each group is given separately
        """
        if schema is None:
            schema = combine_schema(self.producers)
        # Split the data into groups based on null values in columns
        for nulls, group in df.groupby(*(pl.col(col).is_null() for col in df.columns)):
            truth_table = {
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

    def df_with_types(self, types: DataTypeSchema | None = None):
        if types is None:
            types = self.type_schema
        return self.comply_to_schema(types).with_columns(types)

    def get_unfinished(
        self,
    ) -> DataFrame:
        assert self.producers, "No producers specified"

        # check if producers are completely finished
        type_schema: DataTypeSchema = self.type_schema
        self.comply_to_schema(type_schema, in_place=True)
        updated_df: DataFrame = self.__df.with_columns(type_schema)
        unfinished: DataFrame = self.unfinished_by_col(updated_df)
        return unfinished

    def filter(self, lst, sort_col="path") -> Iterable[str]:
        assert sort_col in self.__df.columns, f"'{sort_col}' is not in {self.__df.columns}"
        if len(self.unready_rules):
            warnings.warn(
                f"{len(self.unready_rules)} filters are not initialized and will not be populated", stacklevel=2
            )

        vdf: DataFrame = self.__df.filter(pl.col("path").is_in(lst))
        combined = self.combine_exprs(self.rules)
        for f in combined:
            vdf = f(vdf, self.__df) if isinstance(f, Comparable) else vdf.filter(f)

        return vdf.sort(sort_col).get_column("path")

    @staticmethod
    def combine_exprs(rules: Iterable[Rule]) -> list[Expr | bool | Comparable]:
        """this combines expressions from different objects to a list of compressed expressions.
        DataRules that are FastComparable can be combined, but Comparables cannot. They will be copied to the list.
        """
        combinations: list[Expr | bool | Comparable] = []
        combination: Expr | bool | None = None
        for rule in rules:
            comparer: Comparable | FastComparable = rule.comparer
            if isinstance(comparer, FastComparable):
                combination = combination & comparer() if combination is not None else comparer()
            elif isinstance(comparer, Comparable):
                if combination is not None:
                    combinations.append(combination)
                    combination = None
                combinations.append(comparer)
        if combination is not None:
            combinations.append(combination)

        return combinations

    def save_df(self, pth: str | Path | None = None) -> None:
        """saves the dataframe to self.filepath"""
        self.__df.write_ipc(pth or self.filepath)

    def update(self, df: DataFrame, on="path", how: Literal["left", "inner"] = "left"):
        self.__df = self.__df.update(df, on=on, how=how)

    @property
    def df(self) -> DataFrame:
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

    def __repr__(self) -> str:
        attrlist: list[str] = [
            f"{key}={val!r}" for key, val in self.__dict__.items() if all(k not in key for k in ("__"))
        ]
        return f"{self.__class__.__name__}({', '.join(attrlist)})"

    @overload
    def comply_to_schema(self, schema: SchemaDefinition, in_place: Literal[True]) -> None:
        ...

    @overload
    def comply_to_schema(self, schema: SchemaDefinition, in_place: Literal[False] = False) -> DataFrame:
        ...

    def comply_to_schema(self, schema: SchemaDefinition, in_place=False) -> DataFrame | None:
        new_df: DataFrame = pl.concat((self.__df, DataFrame(schema=schema)), how="diagonal")
        if in_place:
            self.__df = new_df
        return new_df


def chunk_split(
    df: DataFrame,
    chunksize: int,
    col_name: str = "_idx",
):
    return (
        ((idx, len(part)), part.drop(col_name))
        for idx, part in df.with_row_count(col_name)
        .with_columns(pl.col(col_name) // chunksize)
        .groupby(col_name, maintain_order=True)
    )


def blacklist_schema(schema: list[ExprDict], blacklist: Collection) -> list[ExprDict]:
    return [out for dct in schema if (out := {k: v for k, v in dct.items() if k not in blacklist})]
