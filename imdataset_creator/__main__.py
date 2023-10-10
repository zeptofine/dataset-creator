from .main import app
from multiprocessing import freeze_support

if __name__ == "__main__":
    freeze_support()
    app()
