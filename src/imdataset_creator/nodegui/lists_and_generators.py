import contextlib
import random
from collections.abc import Generator
from pathlib import Path
from queue import Empty, Queue

import wcmatch.glob as wglob
from qtpy.QtGui import QFont, QFontMetrics
from qtpy.QtWidgets import (
    QLabel,
    QSpinBox,
    QToolButton,
    QWidget,
)
from qtpynodeeditor import (
    ConnectionPolicy,
    DataTypes,
    NodeData,
    NodeDataModel,
    NodeValidationState,
    Port,
    PortCount,
)

from ..datarules.base_rules import flags
from ..gui.input_view import DEFAULT_IMAGE_FORMATS
from ..gui.settings_inputs import DirectoryInput, MultilineInput, SettingsBox
from .base_types import (
    ListData,
    PathData,
    PathGeneratorData,
)


class UnreadyError(Exception):
    ...


def get_text_bounds(text: str, font: QFont):
    return QFontMetrics(font).size(0, text, 0)


class FileGlobber(NodeDataModel):
    num_ports = PortCount(0, 1)

    all_data_types = PathGeneratorData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._widget = SettingsBox(
            {
                "path": DirectoryInput().label("Folder"),
                "expressions": MultilineInput(
                    default="\n".join(f"**/*{ext}" for ext in DEFAULT_IMAGE_FORMATS), is_list=True
                ),
            }
        )
        self._start_button = QToolButton()
        self._start_button.clicked.connect(lambda: self.get_generator())
        self._start_button.setText("gather")
        self._widget.layout().addWidget(self._start_button)
        self._result = None
        self._saved_generator = None

    def out_data(self, _: int) -> NodeData | None:
        if self._saved_generator is None:
            return None
        return PathGeneratorData(self._saved_generator)

    def get_generator(self):
        cfg = self._widget.get_cfg()
        path = cfg["path"]
        expressions = cfg["expressions"]
        self._saved_generator = self._generator(Path(path), expressions)
        self.data_updated.emit(0)

    def _generator(self, path: Path, expressions):
        for file in wglob.iglob(expressions, flags=flags, root_dir=path):
            yield path / file

    def embedded_widget(self) -> QWidget:
        return self._widget

    def port_out_connection_policy(self, _: int) -> ConnectionPolicy:
        return ConnectionPolicy.one


class GeneratorStepper(NodeDataModel):
    num_ports = PortCount(1, 1)
    data_types = DataTypes(
        {
            0: PathGeneratorData.data_type,
        },
        {0: PathData.data_type},
    )

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._generator = None
        self._validation_state = NodeValidationState.error
        self._validation_message = "Uninitialized"
        self._step_button = QToolButton()
        self._step_button.clicked.connect(self.pop_data)
        self._next_item = None

    def out_data(self, port: int) -> PathData | None:
        if self._next_item is None:
            return None
        return PathData(self._next_item or Path())

    def pop_data(self):
        if self._generator is None:
            return
        try:
            self._next_item = next(self._generator)
            self.data_updated.emit(0)
        except StopIteration:
            self._step_button.setEnabled(False)
            self.invalidate()

    def set_in_data(self, node_data: PathGeneratorData | None, port: Port):
        if node_data is None:
            self.invalidate()
            self._step_button.setEnabled(False)
            return

        self._generator = node_data.generator
        self._step_button.setEnabled(True)
        self.revalidate()

    def invalidate(self):
        self._validation_state = NodeValidationState.error
        self._validation_message = "input generator is empty"

    def revalidate(self):
        self._validation_state = NodeValidationState.valid
        self._validation_message = ""

    def validation_state(self) -> NodeValidationState:
        return self._validation_state

    def validation_message(self) -> str:
        return self._validation_message

    def embedded_widget(self) -> QWidget:
        return self._step_button


class GeneratorSplitterDataModel(NodeDataModel):
    """splits a generator using two deques"""

    all_data_types = PathGeneratorData.data_type
    num_ports = PortCount(1, 2)

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._first_q = Queue()
        self._last_q = Queue()
        self._in_gen: Generator | None = None
        self._label = QLabel()

    def out_data(self, port: int) -> PathGeneratorData:
        if port == 0:
            return PathGeneratorData(self._generator(self._first_q))

        return PathGeneratorData(self._generator(self._last_q))

    def set_in_data(self, node_data: PathGeneratorData | None, _: Port):
        if node_data is None:
            return

        self.update_label()

        with self._first_q.mutex, self._last_q.mutex:
            self._first_q.queue.clear()
            self._last_q.queue.clear()

        self._in_gen = node_data.generator
        self.data_updated.emit(0)
        self.data_updated.emit(1)

    def get_next_item(self):
        assert self._in_gen is not None
        with contextlib.suppress(StopIteration):
            next_item = next(self._in_gen)
            self._first_q.put(next_item)
            self._last_q.put(next_item)

    def _generator(self, q: Queue):
        assert self._in_gen is not None
        while True:
            if q.empty():
                self.get_next_item()
                if q.empty():
                    break

            try:
                item = q.get()
            except Empty:
                break
            self.update_label()
            yield item

    def update_label(self):
        newtxt = f"{self._first_q.qsize()}\n{self._last_q.qsize()}"
        self._label.setText(newtxt)
        self._label.setFixedSize(get_text_bounds(newtxt, self._label.font()))

    def port_out_connection_policy(self, _: int) -> ConnectionPolicy:
        return ConnectionPolicy.one

    def embedded_widget(self) -> QWidget:
        return self._label


class GeneratorResolverDataModel(NodeDataModel):
    num_ports = PortCount(1, 1)
    data_types = DataTypes(
        {
            0: PathGeneratorData.data_type,
        },
        {
            0: ListData.data_type,
        },
    )

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._list = None

    def out_data(self, port: int) -> NodeData | None:
        if self._list is None:
            return None
        return ListData(self._list)

    def set_in_data(self, node_data: PathGeneratorData | None, port: Port):
        if node_data is None:
            return
        self._list = list(node_data.generator)
        self.data_updated.emit(0)


class ListHeadDataModel(NodeDataModel):
    num_ports = PortCount(1, 1)
    all_data_types = ListData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._num = QSpinBox()
        self._num.setValue(3)
        self._num.valueChanged.connect(lambda: self.data_updated.emit(0))
        self._in_list = []

    def out_data(self, port: int) -> NodeData | None:
        if self._in_list is None:
            return None
        return ListData(self._in_list[: self._num.value()])

    def set_in_data(self, node_data: ListData | None, port: Port):
        if node_data is None:
            return

        self._in_list = node_data.list
        self.data_updated.emit(0)

    def embedded_widget(self) -> QWidget:
        return self._num


class ListShufflerDataModel(NodeDataModel):
    caption = "Shuffle List"
    all_data_types = ListData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._list = []

    def out_data(self, port: int) -> NodeData | None:
        if self._list is None:
            return None
        return ListData(sorted(self._list, key=lambda _: random.random()))

    def set_in_data(self, node_data: ListData | None, port: Port):
        if node_data is None:
            self._list = None
            return
        self._list = node_data.list
        self.data_updated.emit(0)


class ListBufferDataModel(NodeDataModel):
    caption = "List Buffer"
    all_data_types = ListData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._list = None
        self._buffer_button = QToolButton()
        self._buffer_button.setText("release")
        self._buffer_button.clicked.connect(lambda: self.data_updated.emit(0))

    def out_data(self, port: int) -> NodeData | None:
        if self._list is None:
            return None
        return ListData(self._list)

    def set_in_data(self, node_data: ListData | None, port: Port):
        if node_data is None:
            self._list = None
            return
        self._list = node_data.list

    def embedded_widget(self) -> QWidget:
        return self._buffer_button
