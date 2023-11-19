from time import sleep

from qtpy.QtCore import QThread
from qtpynodeeditor import (
    CaptionOverride,
    DataTypes,
    NodeData,
    NodeDataModel,
    Port,
    PortCount,
    PortType,
)

from .base_types.base_types import (
    AnyData,
    BoolData,
    SignalData,
)


class NotNode(NodeDataModel):
    caption = "NOT"
    all_data_types = BoolData.data_type
    num_ports = PortCount(1, 1)
    caption_override = CaptionOverride({0: "In"}, {0: "Out"})

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._result: bool | None = None

    def out_data(self, port: int) -> NodeData | None:
        if self._result is None:
            return None
        return BoolData(self._result)

    def set_in_data(self, node_data: BoolData | None, port: Port):
        if node_data is not None:
            self._result = not node_data.value
            self.data_updated.emit(0)


class BufferNode(NodeDataModel):
    caption = "Buffer"
    num_ports = PortCount(2, 2)
    data_types = DataTypes(
        {
            0: AnyData.data_type,
            1: SignalData.data_type,
        },
        {
            0: AnyData.data_type,
            1: SignalData.data_type,
        },
    )

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._item = None

    def out_data(self, port: int) -> AnyData | None:
        if self._item is None:
            return None
        return AnyData(self._item)

    def set_in_data(self, node_data: AnyData | None, port: Port):
        if node_data is None:
            self._item = None
            return
        if port.index == 0:
            self._item = node_data.value
        elif port.index == 1:
            self.data_updated.emit(0)
            self.data_updated.emit(1)


class SwitchCaseNode(NodeDataModel):
    num_ports = PortCount(3, 2)
    data_types = DataTypes(
        {
            0: AnyData.data_type,
            1: BoolData.data_type,
            2: SignalData.data_type,
        },
        {
            0: AnyData.data_type,
            1: AnyData.data_type,
        },
    )
    caption_override = CaptionOverride(
        inputs={
            0: "Data",
            1: "Condition",
            2: "Execute",
        },
        outputs={
            0: "Then",
            1: "Else",
        },
    )
    caption_visible = True

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._bool = None
        self._item = None

    def out_data(self, port: int) -> AnyData | None:
        if self._item is None:
            return None
        return AnyData(self._item)

    def set_in_data(self, node_data: AnyData | BoolData | None, port: Port):
        if node_data is None:
            self._item = None
            return
        if port.index == 0 and isinstance(node_data, AnyData):
            self._item = node_data.value
        elif port.index == 1 and isinstance(node_data, BoolData):
            self._bool = node_data.value
        elif port.index == 2 and self._bool is not None and self._item is not None:  # reset
            self.data_updated.emit(int(not self._bool))


class DistributorNode(NodeDataModel):
    num_ports = PortCount(1, 2)
    all_data_types = AnyData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._n = 0
        self._item = None
        self._released = False

    def out_data(self, port: int) -> NodeData | None:
        if self._item is None:
            return None
        if self._released:
            self._released = False
            return AnyData(self._item)
        return None

    def set_in_data(self, node_data: AnyData | None, port: Port):
        if node_data is None:
            return

        self._item = node_data.value
        self._released = True
        self.data_updated.emit(self._n)
        self._n = (self._n + 1) % 2


class DelayThread(QThread):
    duration: float

    def run(self):
        sleep(self.duration)


class DelayNode(NodeDataModel):
    num_ports = PortCount(1, 1)
    all_data_types = AnyData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._thread = DelayThread(parent)
        self._thread.finished.connect(lambda: self.data_updated.emit(0))
        self._in = None

    def out_data(self, port: int) -> NodeData | None:
        return self._in

    def set_in_data(self, node_data: NodeData | None, port: Port):
        if node_data is None:
            return
        self._in = node_data
        self._thread.start()


class MergeLaneNode(NodeDataModel):
    num_ports = PortCount(2, 1)
    all_data_types = AnyData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._result = None

    def out_data(self, port: int) -> NodeData | None:
        return AnyData(self._result)

    def set_in_data(self, node_data: AnyData | None, port: Port):
        if node_data is None:
            return
        self._result = node_data.value
        self.data_updated.emit(0)


class SplitLaneNode(NodeDataModel):
    num_ports = PortCount(1, 4)
    all_data_types = AnyData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._result = None
        self._output_count = 1

    def out_data(self, port: int) -> NodeData | None:
        return self._result

    def set_in_data(self, node_data: AnyData | None, port: Port):
        if node_data is None:
            return
        self._result = AnyData(node_data.value)
        self.data_updated.emit(0)
        self.data_updated.emit(1)
        self.data_updated.emit(2)
        self.data_updated.emit(3)


class BoolToSignalNode(NodeDataModel):
    num_ports = PortCount(1, 1)
    data_types = DataTypes(
        {
            0: BoolData.data_type,
        },
        {
            0: SignalData.data_type,
        },
    )

    def out_data(self, port: int) -> NodeData | None:
        return SignalData()

    def set_in_data(self, node_data: BoolData | None, port: Port):
        if node_data is None:
            return

        if node_data.value:
            self.data_updated.emit(0)


ALL_MODELS = [
    NotNode,
    DelayNode,
    BufferNode,
    SwitchCaseNode,
    DistributorNode,
    MergeLaneNode,
    SplitLaneNode,
    BoolToSignalNode,
]
