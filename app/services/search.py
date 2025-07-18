import io
import logging
import re
import os
from typing import List, Dict, Union
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import cv2
import fitz  # PyMuPDF
import pytesseract

# --- Your Extractor utils. Make sure these exist or adapt as needed ---
# Example stubs below:
DPI = 150
TESSERACT_CONFIG = '--oem 1 --psm 6 -c preserve_interword_spaces=1'
MIN_TEXT_LENGTH = 50

def clean_ocr_text(text: str) -> str:
    # Remove suspicious whitespace and normalize newlines.
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n+', '\n', text)
    text = re.sub(r'^[ \t]+|[ \t]+$', '', text, flags=re.MULTILINE)
    return text.strip()

def fast_preprocess(img_array):
    gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
    return cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]

def page_pixmap_to_image(pix):
    """
    Converts a PyMuPDF pixmap to numpy OpenCV image.
    """
    arr = np.frombuffer(pix.samples, dtype=np.uint8)
    if pix.n >= 4:
        # RGBA
        img = arr.reshape((pix.h, pix.w, pix.n))
        img = cv2.cvtColor(img, cv2.COLOR_RGBA2RGB)
    else:
        img = arr.reshape((pix.h, pix.w, pix.n))
    return img
# ---------------------------------------------------------------------

logger = logging.getLogger(__name__)

def process_page_and_search(
    page_num: int, pdf_bytes: bytes, keywords: List[str],
    return_only_filtered: bool, dpi: int = DPI
) -> Union[Dict, None]:
    """
    Extracts digital text if present, else runs OCR on the page image.
    Returns a search result dict or None if not required.
    """
    try:
        with fitz.open("pdf", pdf_bytes) as doc:
            page = doc.load_page(page_num)
            text = page.get_text()
            # If text is insufficient, run OCR on rendered image
            if len(text.strip()) < MIN_TEXT_LENGTH:
                pix = page.get_pixmap(dpi=dpi)
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
        logger.error(f"Error processing/searching page {page_num}: {e}")
    return None

def search_keywords_live_parallel(
    pdf_bytes: bytes,
    keywords: List[str],
    return_only_filtered: bool = False,
    THREADS: int = 8
) -> Dict[str, Union[List[Dict], Dict]]:
    """
    Process all PDF pages in memory in parallel using PyMuPDF and OCR.
    :param pdf_bytes: PDF in bytes (decoded from base64)
    :param keywords: List of keywords to search
    :param return_only_filtered: Return only pages with matched keywords
    :param THREADS: Number of threads to use
    :return: OCR results with matched pages
    """

    results = []

    try:
        # Load the in-memory PDF and get page count
        with fitz.open("pdf", pdf_bytes) as doc:
            num_pages = len(doc)

        # Parallel processing of each page
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = [
                executor.submit(
                    process_page_and_search,
                    i,
                    pdf_bytes,
                    keywords,
                    return_only_filtered
                )
                for i in range(num_pages)
            ]

            for future in futures:
                result = future.result()
                if result:
                    results.append(result)

        # If no results and we only want matched pages
        if not results and return_only_filtered:
            return {
                "imageToTextSearchResponse": {
                    "keywordMatched": False,
                    "selectedKeywords": "NOT FOUND",
                    "pageContent": "null"
                }
            }

        return {"imageToTextSearchResponse": results}

    except Exception as e:
        logger.error(f"Error in base64 OCR processor: {e}")
        return {
            "imageToTextSearchResponse": {
                "keywordMatched": False,
                "selectedKeywords": "ERROR",
                "pageContent": str(e)
            }
        }

