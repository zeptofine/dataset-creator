import contextlib
import shutil
from abc import abstractmethod
from collections.abc import Callable
from operator import attrgetter
from pathlib import Path
from typing import Any

from PySide6.QtCore import Slot
from qtpy.QtWidgets import QLineEdit, QWidget
from qtpynodeeditor import (
    DataTypes,
    NodeData,
    NodeDataModel,
    NodeValidationState,
    Port,
    PortCount,
)

from ..datarules.base_rules import Input
from ..gui.config_inputs import ItemDeclaration
from ..gui.settings_inputs import (
    DirectoryInput,
    FileInput,
)
from .base_types.base_types import BoolData, PathData, SignalData, StringData


class FileNode(NodeDataModel):
    name = "File"
    caption = "File"
    num_ports = PortCount(0, 1)
    all_data_types = PathData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._item = self.get_item()
        self._widget = self._item.create_settings_widget()
        row = self._widget.rows["path"]

        line_edit: QLineEdit = row.widgets[0]  # type: ignore
        self.line_edit: QLineEdit = line_edit
        self.line_edit.textChanged.connect(self.update)
        self._path = None

    def out_data(self, _: int) -> PathData | None:
        if self._path is None:
            return None
        return PathData(self._path)

    def get_item(self):
        return ItemDeclaration(
            "",
            Input,
            settings={
                "path": FileInput(),
            },
        )

    def save(self) -> dict:
        dct = super().save()
        if self._path is not None:
            dct["settings"] = self._widget.get_cfg()
            dct["path"] = str(self._path)
        return dct

    def restore(self, doc: dict):
        if "path" in doc:
            self._widget.from_cfg(doc)
            self._path = Path(doc["path"])
            self.line_edit.setText(str(self._path))

    @Slot(str)
    def update(self, txt):
        self._path = Path(txt).expanduser()
        self.data_updated.emit(0)

    def embedded_widget(self) -> QWidget:
        return self._widget


class FolderNode(FileNode):
    name = "Folder"
    caption = "Folder"

    def get_item(self):
        return ItemDeclaration(
            "",
            Input,
            settings={
                "path": DirectoryInput(),
            },
        )


# TODO: Think of better names for these classes
class BasicPathAttrGetterNode(NodeDataModel):
    data_types = DataTypes(
        {0: PathData.data_type},
        {0: StringData.data_type},
    )
    num_ports = PortCount(1, 1)

    attrgetter_: attrgetter

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._output: StringData | None = None
        self._validation_state = NodeValidationState.valid
        self._validation_message = "what"

    def out_data(self, _: int) -> NodeData | None:
        if self._output is None:
            return None
        return self._output

    def set_in_data(self, node_data: PathData | None, _: Port):
        if node_data is None:
            self._validation_state = NodeValidationState.valid
            self._validation_message = ""
            return

        with self.validatable_context():
            attr = self.compute(node_data.value)
            self._output = StringData(str(attr))
            self.data_updated.emit(0)

    @staticmethod
    def compute(path: Path) -> str:
        ...

    @contextlib.contextmanager
    def validatable_context(self):
        try:
            yield
        except Exception as e:
            self._validation_state = NodeValidationState.error
            self._validation_message = str(e)
            raise
        else:
            self._validation_state = NodeValidationState.valid
            self._validation_message = ""

    def validation_state(self) -> NodeValidationState:
        return self._validation_state

    def validation_message(self) -> str:
        return self._validation_message


class BasicPathRelativeGetterNode(BasicPathAttrGetterNode):
    data_types = DataTypes(
        {0: PathData.data_type},
        {0: PathData.data_type},
    )
    _output: PathData | None

    def set_in_data(self, node_data: PathData | None, _: Port):
        if node_data is None:
            return

        with self.validatable_context():
            attr = self.compute(node_data.value)
            self._output = PathData(attr)
            self.data_updated.emit(0)

    @staticmethod
    def compute(path: Path) -> Path:
        ...


class BasicPathCheckerNode(BasicPathRelativeGetterNode):
    data_types = DataTypes(
        {0: PathData.data_type},
        {0: BoolData.data_type},
    )
    _output: BoolData | None

    def set_in_data(self, node_data: PathData | None, _: Port):
        if node_data is None or node_data.value is None:
            return
        with self.validatable_context():
            attr = self.compute(node_data.value)
            self._output = BoolData(attr)
            self.data_updated.emit(0)

    @staticmethod
    def compute(p: Path) -> bool:
        ...


class BasicPathSingleComputerNode(BasicPathRelativeGetterNode):
    num_ports = PortCount(2, 1)
    data_types = DataTypes(
        {0: PathData.data_type, 1: SignalData.data_type},
        {0: SignalData.data_type},
    )

    def out_data(self, _: int) -> NodeData | None:
        return SignalData()

    def set_in_data(self, node_data: PathData | None, port: Port):
        if node_data is None:
            return
        if port.index == 0:
            self._first = node_data.value

        if port.index == 1 and self._first is not None:
            with self.validatable_context():
                self.compute(self._first)
                self.data_updated.emit(0)

    @staticmethod
    def compute(p: Path) -> None:
        ...


class BasicPathTransformerNode(BasicPathRelativeGetterNode):
    num_ports = PortCount(2, 1)
    data_types = DataTypes(
        {0: PathData.data_type, 1: PathData.data_type},
        {0: PathData.data_type},
    )

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._first = None
        self._second = None

    def set_in_data(self, node_data: PathData | None, port: Port):
        if node_data is None:
            if port.index == 0:
                self._first = None
            if port.index == 1:
                self._second = None
            return
        if port.index == 0:
            self._first = node_data.value
        if port.index == 1:
            self._second = node_data.value

        if self._first is not None and self._second is not None:
            with self.validatable_context():
                out = self.compute(self._first, self._second)
                self._output = PathData(out)
                self.data_updated.emit(0)

    @staticmethod
    def compute(first: Path, second: Path) -> Path:
        ...


class BasicPathMoverNode(BasicPathTransformerNode):
    num_ports = PortCount(3, 1)
    data_types = DataTypes(
        {0: PathData.data_type, 1: PathData.data_type, 2: SignalData.data_type},
        {0: BoolData.data_type},
    )

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self.success = None

    def out_data(self, _: int) -> NodeData | None:
        if self.success is None:
            return None
        return BoolData(self.success)

    def set_in_data(self, node_data: PathData | None, port: Port):
        if node_data is None:
            return
        if port.index == 0:
            self._first = node_data.value
        if port.index == 1:
            self._second = node_data.value

        if port.index == 2 and self._first is not None and self._second is not None:
            try:
                with self.validatable_context():
                    self.compute(self._first, self._second)
            except Exception as e:
                self.success = False
            else:
                self.success = True

            self.data_updated.emit(0)

    @staticmethod
    @abstractmethod
    def compute(first: Path, second: Path) -> None:
        ...


def new_pathcomputer_model(attr: str, computer: Callable, out_type: type[str] | type[Path] = str):
    attrs = {
        "name": f"(Path) {attr}",
        "caption": f"(Path) {attr}",
        "compute": staticmethod(computer),
    }

    if out_type is str:
        return type(attr, (BasicPathAttrGetterNode,), attrs)
    return type(attr, (BasicPathRelativeGetterNode,), attrs)


def new_pathtransformer_model(name: str, method: Callable[[Path, Path], Path]):
    return type(
        method.__name__,
        (BasicPathTransformerNode,),
        {
            "name": name,
            "caption": name,
            "compute": staticmethod(method),
        },
    )


def new_pathsinglecompute_model(name: str, method: Callable[[Path], Any]):
    return type(
        method.__name__,
        (BasicPathSingleComputerNode,),
        {
            "name": name,
            "caption": name,
            "compute": staticmethod(method),
        },
    )


def new_pathchecker_model(name: str, method: Callable[[Path], bool]):
    return type(
        method.__name__,
        (BasicPathCheckerNode,),
        {
            "name": name,
            "caption": name,
            "compute": staticmethod(method),
        },
    )


def new_pathmover_model(name: str, method: Callable[[Path, Path], Path]):
    return type(
        method.__name__,
        (BasicPathMoverNode,),
        {
            "name": name,
            "caption": name,
            "compute": staticmethod(method),
        },
    )


ALL_MODELS = (
    [FileNode, FolderNode]
    + [new_pathcomputer_model(attr, attrgetter(attr)) for attr in ["stem", "name", "suffix", "stem"]]
    + [new_pathcomputer_model(attr, attrgetter(attr), out_type=Path) for attr in ["parent"]]
    + [
        new_pathsinglecompute_model(name, method)
        for name, method in {
            "(Path) absolute": Path.absolute,
            "(Path) resolve": Path.resolve,
            "(Path) makedir": lambda p: Path.mkdir(p, parents=True, exist_ok=True),
        }.items()
    ]
    + [
        new_pathtransformer_model(name, method)
        for name, method in {
            "(Path) Relative to": Path.relative_to,
            "(Path) joinpath": Path.joinpath,
        }.items()
    ]
    + [
        new_pathchecker_model(name, method)
        for name, method in {
            "(Path) Exists?": Path.exists,
            "(Path) Is dir?": Path.is_dir,
            "(Path) Is file?": Path.is_file,
            "(Path) Is absolute?": Path.is_absolute,
        }.items()
    ]
    + [
        new_pathmover_model(name, method)
        for name, method in {
            "(Path) rename": Path.rename,
            "(Path) replace": Path.replace,
            "(Path) link to": Path.symlink_to,
            "(shutil) move": shutil.move,
            "(shutil) copy": shutil.copy,
            "(shutil) copytree": shutil.copytree,
        }.items()
    ]
)
