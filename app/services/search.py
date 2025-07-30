import logging
import re
import os
from typing import List, Dict, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import cv2
import fitz  # PyMuPDF
import pytesseract

DPI = 150
TESSERACT_CONFIG = '--oem 1 --psm 6'
MIN_TEXT_LENGTH = 40
THREADS = os.cpu_count() or 4
# Tesseract path for Windows users
# pytesseract.pytesseract.tesseract_cmd = r'C:\Users\st\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'
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
    arr = np.frombuffer(pix.samples, dtype=np.uint8)
    if pix.n >= 4:
        img = arr.reshape((pix.h, pix.w, pix.n))
        img = cv2.cvtColor(img, cv2.COLOR_RGBA2RGB)
    else:
        img = arr.reshape((pix.h, pix.w, pix.n))
    return img

def process_page_from_doc(
    page_num: int,
    page: fitz.Page,
    keywords: List[str],
    return_only_filtered: bool
) -> Union[Dict, None]:
    try:
        text = page.get_text()
        if len(text.strip()) < MIN_TEXT_LENGTH:
            pix = page.get_pixmap(dpi=DPI)
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
        logger.error(f"[Page {page_num}] Error: {e}")
    return None

def search_keywords_live_parallel(
    pdf_bytes: bytes,
    keywords: List[str],
    return_only_filtered: bool = False,
    THREADS: int = THREADS
) -> Dict[str, Union[List[Dict], Dict]]:
    results = []
    doc = None
    try:
        doc = fitz.open("pdf", pdf_bytes)
        pages = [doc.load_page(i) for i in range(len(doc))]
        logger.info(f"Loaded {len(pages)} pages; ThreadPoolExecutor with {THREADS} threads")

        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = [
                executor.submit(
                    process_page_from_doc,
                    i,
                    pages[i],
                    keywords,
                    return_only_filtered
                )
                for i in range(len(pages))
            ]
            for future in as_completed(futures):
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
        results.sort(key=lambda x: x['pageNO'])
        return {"imageToTextSearchResponse": results}
    except Exception as e:
        logger.exception("OCR processing failed!")
        return {
            "imageToTextSearchResponse": [{
                "pageNO": 0,
                "keywordMatched": False,
                "selectedKeywords": "ERROR",
                "pageContent": str(e)
            }]
        }
    finally:
        if doc is not None:
            doc.close()
