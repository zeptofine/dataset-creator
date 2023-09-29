import cv2
import numpy as np
from PIL import Image

from ..datarules.base_rules import Filter


class Resize(Filter):
    def run(self, img: np.ndarray, scale: float) -> np.ndarray:
        """
        Resize the image.

        :param img: The image to resize.
        :param scale: The scale to resize.
        :return: The resized image.
        """
        return cv2.resize(img, (int(img.shape[1] * scale), int(img.shape[0] * scale)))
