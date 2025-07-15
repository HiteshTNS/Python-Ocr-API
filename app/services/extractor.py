import logging
import re
import cv2
import numpy as np

DPI = 150
TESSERACT_CONFIG = '--oem 1 --psm 6 -c preserve_interword_spaces=1'
MIN_TEXT_LENGTH = 50

logger = logging.getLogger(__name__)

def clean_ocr_text(text: str) -> str:
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n+', '\n', text)
    text = re.sub(r'^[ \t]+|[ \t]+$', '', text, flags=re.MULTILINE)
    return text.strip()

def fast_preprocess(img_array):
    gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
    return cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]

def page_pixmap_to_image(pix):
    return np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
