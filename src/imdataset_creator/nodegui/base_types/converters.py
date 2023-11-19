from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

import qtpynodeeditor as ne
from qtpynodeeditor import (
    NodeData,
    NodeDataType,
)
from qtpynodeeditor.type_converter import TypeConverter

from .base_types import (
    AnyData,
    BoolData,
    FloatData,
    ImageData,
    IntegerData,
    ListData,
    PathData,
    PathGeneratorData,
    RandomNumberGeneratorData,
    SignalData,
    StringData,
)


def gen2list(data: PathGeneratorData) -> ListData:
    return ListData(list(data.value))


def list2gen(data: ListData) -> PathGeneratorData:
    return PathGeneratorData(x for x in data.value)


def any_to_generator_converter(data: AnyData):
    return PathGeneratorData(Path(p) for p in data.value)


def anything_to_signal_converter(_: NodeData) -> SignalData:
    return SignalData()


def register_type(registry: ne.DataModelRegistry, from_: NodeDataType, to_: NodeDataType, converter: Callable):
    registry.register_type_converter(from_, to_, TypeConverter(from_, to_, converter))


_NodeT1 = TypeVar("_NodeT1", bound=NodeData)
_NodeT2 = TypeVar("_NodeT2", bound=NodeData)


def register_type2(
    registry: ne.DataModelRegistry,
    from_: type[_NodeT1],
    to_: type[_NodeT2],
    converter: Callable[[_NodeT1], _NodeT2],
):
    registry.register_type_converter(
        from_.data_type, to_.data_type, TypeConverter(from_.data_type, to_.data_type, converter)
    )


def bool_to_signal_generator(item: BoolData) -> SignalData | None:
    return SignalData() if item.value else None


T = TypeVar("T")


def any_to_type(t: type[T]) -> Callable[[AnyData], T]:
    def f(data: AnyData):
        return t(data.value)

    return f


def int2rng(item: IntegerData) -> RandomNumberGeneratorData:
    return RandomNumberGeneratorData(lambda: item.value)


def float2rng(item: FloatData) -> RandomNumberGeneratorData:
    return RandomNumberGeneratorData(lambda: item.value)


def bool2float(b: BoolData) -> FloatData:
    return FloatData(float(b.value))


def bool2int(b: BoolData) -> IntegerData:
    return IntegerData(int(b.value))


def truthy2bool(f: FloatData | IntegerData) -> BoolData:
    return BoolData(bool(f.value))


def path2string(p: PathData) -> StringData:
    return StringData(str(p.value))


def string2path(s: StringData) -> PathData:
    return PathData(Path(s.value))


def int2float(i: IntegerData) -> FloatData:
    return FloatData(float(i.value))


def float2int(f: FloatData) -> IntegerData:
    return IntegerData(int(f.value))


def register_types(registry: ne.DataModelRegistry):
    # I hate Any
    for data in (
        PathGeneratorData,
        StringData,
        ListData,
        ImageData,
        PathData,
        BoolData,
    ):
        register_type2(registry, AnyData, data, any_to_type(data))
    register_type2(registry, AnyData, IntegerData, lambda item: IntegerData(int(item.value)))
    register_type2(registry, AnyData, FloatData, lambda item: FloatData(float(item.value)))

    register_type2(registry, ListData, AnyData, lambda item: AnyData(item.value))
    register_type2(registry, PathData, AnyData, lambda item: AnyData(item.value))
    register_type2(registry, ImageData, AnyData, lambda item: AnyData(item.value))
    register_type2(registry, SignalData, AnyData, lambda _: AnyData(True))
    register_type2(registry, IntegerData, AnyData, lambda item: AnyData(item.value))
    register_type2(registry, FloatData, AnyData, lambda item: AnyData(item.value))

    register_type2(registry, SignalData, BoolData, lambda _: BoolData(True))

    register_type2(registry, BoolData, IntegerData, bool2int)
    register_type2(registry, BoolData, FloatData, bool2float)
    register_type2(registry, BoolData, AnyData, lambda item: AnyData(item.value))
    for data in (
        PathGeneratorData,
        ListData,
        AnyData,
        ImageData,
        PathData,
        StringData,
        FloatData,
        IntegerData,
    ):
        register_type2(registry, data, SignalData, anything_to_signal_converter)
    types: dict[tuple[type[NodeData], type[NodeData]], Callable] = {
        (PathGeneratorData, ListData): gen2list,
        (ListData, PathGeneratorData): list2gen,
        (PathData, StringData): path2string,
        (PathData, AnyData): lambda item: AnyData(item.value),
        (StringData, PathData): string2path,
        (StringData, AnyData): lambda item: AnyData(item.value),
        (IntegerData, FloatData): int2float,
        (FloatData, IntegerData): float2int,
        (IntegerData, RandomNumberGeneratorData): int2rng,
        (FloatData, RandomNumberGeneratorData): float2rng,
    }
    for (t1, t2), method in types.items():
        register_type2(registry, t1, t2, method)
