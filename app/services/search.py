import logging
import re
import os

from app.services.extractor import clean_ocr_text, fast_preprocess, page_pixmap_to_image
os.environ['OMP_THREAD_LIMIT'] = '1'
from typing import List, Dict, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import cv2
import fitz  # PyMuPDF
import pytesseract

# --- Configuration ---
DPI = 150
TESSERACT_CONFIG = '--oem 1 --psm 6 -c preserve_interword_spaces=1'
MIN_TEXT_LENGTH = 50
THREADS = os.cpu_count() or 4  # Adjustable

# Tesseract path for Windows users
pytesseract.pytesseract.tesseract_cmd = r'C:\Users\st\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'

# Logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# --- Page Processor for Threads ---
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
            kw for kw in keywords
            if re.search(rf'\b{re.escape(kw)}\b', cleaned, flags=re.IGNORECASE)
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

# --- Main Controller ---
def search_keywords_live_parallel(
    pdf_bytes: bytes,
    keywords: List[str],
    return_only_filtered: bool = False,
    THREADS: int = THREADS
) -> Dict[str, Union[List[Dict], Dict]]:
    results = []
    doc = None

    try:
        doc = fitz.open("pdf", pdf_bytes)  # Do not use 'with' — we need to keep it open
        pages = [doc.load_page(i) for i in range(len(doc))]

        logger.info(f"Loaded {len(pages)} pages; starting ThreadPoolExecutor with {THREADS} threads")

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
                "imageToTextSearchResponse": {
                    "message": "No keywords matched on any page.",
                    "keywordMatched": False,
                    "selectedKeywords": "",
                    "pageContent": ""
                }
            }

        return {"imageToTextSearchResponse": results}

    except Exception as e:
        logger.exception(" OCR processing failed!")
        return {
            "imageToTextSearchResponse": {
                "message": "Unexpected error during OCR processing.",
                "keywordMatched": False,
                "selectedKeywords": "ERROR",
                "pageContent": str(e)
            }
        }
    finally:
        if doc is not None:
            doc.close()  # 🔒 Ensure we close the document
