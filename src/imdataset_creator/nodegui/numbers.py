import operator
import random
from collections.abc import Callable

from qtpy.QtWidgets import (
    QComboBox,
    QDoubleSpinBox,
    QSpinBox,
    QWidget,
)
from qtpynodeeditor import (
    CaptionOverride,
    DataTypes,
    NodeData,
    NodeDataModel,
    NodeValidationState,
    Port,
    PortCount,
)

from .base_types.base_types import (
    BoolData,
    FloatData,
    IntegerData,
    RandomNumberGeneratorData,
    SignalData,
    ValuedNodeData,
)


class IntegerNode(NodeDataModel):
    all_data_types = IntegerData.data_type
    num_ports = PortCount(0, 1)

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._widget = QSpinBox(parent)
        self._widget.valueChanged.connect(self.update)
        self._widget.setMinimum(-1_000_000_000)
        self._widget.setMaximum(1_000_000_000)

    def out_data(self, port: int) -> NodeData | None:
        return IntegerData(self._widget.value())

    def update(self):
        self.data_updated.emit(0)

    def save(self) -> dict:
        dct = super().save()
        dct["value"] = self._widget.value()
        return dct

    def restore(self, doc: dict):
        if "value" in doc:
            self._widget.setValue(doc["value"])

    def embedded_widget(self) -> QWidget:
        return self._widget


class FloatNode(NodeDataModel):
    all_data_types = FloatData.data_type
    num_ports = PortCount(0, 1)

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._widget = QDoubleSpinBox(parent)
        self._widget.valueChanged.connect(self.update)
        self._widget.setMinimum(-1_000_000_000)
        self._widget.setMaximum(1_000_000_000)

    def out_data(self, port: int) -> NodeData | None:
        return FloatData(self._widget.value())

    def update(self):
        self.data_updated.emit(0)

    def save(self) -> dict:
        dct = super().save()
        dct["value"] = self._widget.value()
        return dct

    def restore(self, doc: dict):
        if "value" in doc:
            self._widget.setValue(doc["value"])

    def embedded_widget(self) -> QWidget:
        return self._widget


class MathNode(NodeDataModel):
    all_data_types = FloatData.data_type

    num_ports = PortCount(2, 1)

    caption_override = CaptionOverride(
        {0: "A", 1: "B"},
        {0: "Out"},
    )

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._widget = QComboBox(parent)
        self.operators: dict[str, Callable[[float, float], float]] = {
            "+": operator.add,
            "-": operator.sub,
            "*": operator.mul,
            "/": operator.truediv,
            "//": operator.floordiv,
            "%": operator.mod,
            "**": operator.pow,
            "max": max,
            "min": min,
        }
        self._widget.addItems(list(self.operators.keys()))
        self.first: float | None = None
        self.second: float | None = None
        self._result: FloatData | None = None

    def out_data(self, port: int) -> NodeData | None:
        return self._result

    def set_in_data(self, node_data: FloatData | None, port: Port):
        if port.index == 0:
            self.first = ValuedNodeData.check(node_data)
        elif port.index == 1:
            self.second = ValuedNodeData.check(node_data)

        if self.first is not None and self.second is not None:
            self._result = FloatData(self.operators[self._widget.currentText()](self.first, self.second))
            self.data_updated.emit(0)

    def save(self) -> dict:
        dct = super().save()
        dct["operator"] = self._widget.currentText()
        return dct

    def restore(self, doc: dict):
        if "operator" in doc:
            self._widget.setCurrentText(doc["operator"])

    def embedded_widget(self) -> QWidget:
        return self._widget


class ComparisonNode(NodeDataModel):
    caption_visible = False
    # all_data_types = FloatData.data_type
    data_types = DataTypes(
        {
            0: FloatData.data_type,
            1: FloatData.data_type,
        },
        {0: BoolData.data_type},
    )
    caption_override = CaptionOverride(
        {0: "A", 1: "B"},
        {0: "Out"},
    )

    num_ports = PortCount(2, 1)

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._widget = QComboBox(parent)
        self.operators: dict[str, Callable[[float, float], bool]] = {
            ">": operator.gt,
            ">=": operator.ge,
            "<": operator.lt,
            "<=": operator.le,
            "==": operator.eq,
            "!=": operator.ne,
        }
        self._widget.addItems(list(self.operators.keys()))
        self.first: float | None = None
        self.second: float | None = None
        self._result: bool | None = None

    def out_data(self, port: int) -> NodeData | None:
        if self._result is None:
            return None
        return BoolData(self._result)
        # return FloatData(self._widget.value())

    def set_in_data(self, node_data: FloatData | None, port: Port):
        if port.index == 0:
            self.first = ValuedNodeData.check(node_data)
        elif port.index == 1:
            self.second = ValuedNodeData.check(node_data)

        if self.first is not None and self.second is not None:
            self._result = self.operators[self._widget.currentText()](self.first, self.second)
            self.data_updated.emit(0)

    def save(self) -> dict:
        dct = super().save()
        dct["operator"] = self._widget.currentText()
        return dct

    def restore(self, doc: dict):
        if "operator" in doc:
            self._widget.setCurrentText(doc["operator"])

    def embedded_widget(self) -> QWidget:
        return self._widget


class RandomRangeNode(NodeDataModel):
    data_types = DataTypes(
        {
            0: FloatData.data_type,  # start
            1: FloatData.data_type,  # stop
            2: FloatData.data_type,  # step
        },
        {
            0: RandomNumberGeneratorData.data_type,
        },
    )
    caption_override = CaptionOverride(
        inputs={
            0: "Start",
            1: "Stop",
            2: "Step",
        }
    )
    num_ports = PortCount(3, 1)

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._start = None
        self._stop = None
        self._step = None
        self._result: Callable[[], float] | None = None
        self._validation_state = NodeValidationState.valid
        self._validation_message = ""
        self.check_validation()

    def out_data(self, port: int) -> NodeData | None:
        if self._result is None:
            return None
        return RandomNumberGeneratorData(self._result)

    def set_in_data(self, node_data: FloatData | None, port: Port):
        if port.index == 0:
            self._start = ValuedNodeData.check(node_data)
        if port.index == 1:
            self._stop = ValuedNodeData.check(node_data)
        if port.index == 2:
            self._step = ValuedNodeData.check(node_data)
        self.check_validation()
        if self._start is not None and self._stop is not None and self._step is not None:
            diff = self._stop - self._start
            start = self._start
            step = self._step
            self._result = lambda: int(((random.random() * diff) + start) / step) * step
        else:
            self._result = None

    def check_validation(self):
        if self._start is None or self._stop is None or self._step is None:
            self._validation_state = NodeValidationState.error
            self._validation_message = "Missing input values"
            return False

        self._validation_state = NodeValidationState.valid
        self._validation_message = ""
        return True

    def validation_state(self) -> NodeValidationState:
        return self._validation_state

    def validation_message(self) -> str:
        return self._validation_message


class NumberGeneratorResolverNode(NodeDataModel):
    num_ports = PortCount(2, 1)
    data_types = DataTypes(
        {
            0: RandomNumberGeneratorData.data_type,
            1: SignalData.data_type,
        },
        {
            0: FloatData.data_type,
        },
    )

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._generator = None
        self._result = None

    def set_in_data(self, node_data: RandomNumberGeneratorData | SignalData | None, port: Port):
        if port.index == 0:
            if node_data is None:
                return
            assert isinstance(node_data, RandomNumberGeneratorData)
            self._generator = node_data.value
        if port.index == 1:
            self.compute()

    def out_data(self, port: int) -> NodeData | None:
        if self._result is None:
            return None
        return FloatData(self._result)

    def compute(self):
        if self._generator is None:
            return
        self._result = self._generator()
        self.data_updated.emit(0)


ALL_MODELS = [
    IntegerNode,
    FloatNode,
    MathNode,
    ComparisonNode,
    RandomRangeNode,
    NumberGeneratorResolverNode,
]
