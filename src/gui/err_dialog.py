from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QLabel,
    QVBoxLayout,
)


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
        if prompt is None:
            self.layout().addWidget(QLabel(f"{exc.__class__.__name__}: {exc}", self))
        else:
            self.layout().addWidget(QLabel(f"{prompt}: {exc.__class__.__name__}({exc})", self))
        self.layout().addWidget(self.buttonBox)


def catch_errors(msg):
    def _catcher(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(e)
                dlg = ErrorDialog(e, msg)
                dlg.exec_()

        return wrapper

    return _catcher
