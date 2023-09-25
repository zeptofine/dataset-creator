import cv2
import numpy as np
from PIL import Image

from ..datarules.base_rules import Filter


def resize(img: np.ndarray, scale: float) -> np.ndarray:
    return cv2.resize(img, (int(img.shape[1] * scale), int(img.shape[0] * scale)))


Filter.register_filter(resize)
