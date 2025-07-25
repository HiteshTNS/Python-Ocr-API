import io
import logging
import re
from typing import List, Dict, Union
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import cv2
import fitz  # PyMuPDF
import pytesseract
import threading
import os

DPI = 150  # Slightly reduced for speed
# TESSERACT_CONFIG = '--oem 1 --psm 6 -c preserve_interword_spaces=1'
TESSERACT_CONFIG = r'--oem 1 --psm 6'  # OEM 1 = LSTM only, PSM 6 = Assume uniform block of text

MIN_TEXT_LENGTH = 40  # Lowered threshold for fallback

logger = logging.getLogger(__name__)

_executor_lock = threading.Lock()
_shared_executor = None

def get_executor(threads=None):
    global _shared_executor
    with _executor_lock:
        if _shared_executor is None or threads is not None:
            _shared_executor = ThreadPoolExecutor(max_workers=threads or (os.cpu_count() or 4))
    return _shared_executor

def clean_ocr_text(text: str) -> str:
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n+', '\n', text)
    text = re.sub(r'^[ \t]+|[ \t]+$', '', text, flags=re.MULTILINE)
    return text.strip()

def fast_preprocess(img_array):
    gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
    return cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]

def page_pixmap_to_image(pix):
    arr = np.frombuffer(pix.samples, dtype=np.uint8)
    img = arr.reshape((pix.h, pix.w, pix.n))
    if pix.n >= 4:
        img = cv2.cvtColor(img, cv2.COLOR_RGBA2RGB)
    return img

def process_page_and_search(page_num: int, pdf_bytes: bytes, keywords: List[str],
                            return_only_filtered: bool, dpi: int = DPI) -> Union[Dict, None]:
    try:
        with fitz.open("pdf", pdf_bytes) as doc:
            page = doc.load_page(page_num)
            text = page.get_text()

            if len(text.strip()) < MIN_TEXT_LENGTH:
                pix = page.get_pixmap(dpi=dpi)
                img = page_pixmap_to_image(pix)
                img = fast_preprocess(img)
                text = pytesseract.image_to_string(img, config=TESSERACT_CONFIG)

            cleaned = clean_ocr_text(text)
            matched_keywords = [
                kw for kw in keywords if re.search(rf'\b{re.escape(kw)}\b', cleaned, flags=re.IGNORECASE)
            ]

            if matched_keywords or not return_only_filtered:
                return {
                    "pageNO": page_num + 1,
                    "keywordMatched": bool(matched_keywords),
                    "selectedKeywords": "|".join(matched_keywords),
                    "pageContent": cleaned.replace("\n", " ")
                }
    except Exception as e:
        logger.error(f"[ERROR] Page {page_num}: {e}")
    return None

def search_keywords_live_parallel(
    pdf_bytes: bytes,
    keywords: List[str],
    return_only_filtered: bool = False,
    THREADS: int = None
) -> Dict[str, Union[List[Dict], Dict]]:
    results = []

    try:
        with fitz.open("pdf", pdf_bytes) as doc:
            num_pages = len(doc)

        executor = get_executor(THREADS)
        futures = [
            executor.submit(process_page_and_search, i, pdf_bytes, keywords, return_only_filtered)
            for i in range(num_pages)
        ]

        for future in futures:
            result = future.result()
            if result:
                results.append(result)

        if not results and return_only_filtered:
            return {
                "imageToTextSearchResponse": [{
                    "pageNO": 0,
                    "keywordMatched": False,
                    "selectedKeywords": "NOT FOUND",
                    "pageContent": "null"
                }]
            }

        return {"imageToTextSearchResponse": results}

    except Exception as e:
        logger.exception("[FATAL] OCR search failed")
        return {
            "imageToTextSearchResponse": [{
                "pageNO": 0,
                "keywordMatched": False,
                "selectedKeywords": "ERROR",
                "pageContent": str(e)
            }]
        }
