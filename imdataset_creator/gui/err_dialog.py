from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QLabel,
    QVBoxLayout,
)
import traceback


class ErrorDialog(QDialog):
    def __init__(
        self,
        exc: Exception,
        prompt=None,
        parent=None,
    ):
        super().__init__(parent)
        self.setWindowTitle(f"Error: {exc.__class__.__name__}")
        self.setLayout(QVBoxLayout(self))
        QBtn = QDialogButtonBox.StandardButton.Ok
        self.buttonBox = QDialogButtonBox(QBtn)
        self.buttonBox.accepted.connect(self.accept)
        exc_text = "".join(traceback.format_exception(exc))
        label_text = (
            f"{exc.__class__.__name__}: {exc_text}"
            if prompt is None
            else f"{prompt}: {exc.__class__.__name__}({exc_text})"
        )
        self.layout().addWidget(QLabel(label_text, self))
        self.layout().addWidget(self.buttonBox)


def catch_errors(msg):
    def _catcher(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                dlg = ErrorDialog(e, msg)
                dlg.exec_()
                raise e

        return wrapper

    return _catcher
