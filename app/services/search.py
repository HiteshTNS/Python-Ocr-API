import io
import logging
import re
from typing import List, Dict, Union
from concurrent.futures import ThreadPoolExecutor

import cv2
import fitz
import numpy as np
import pytesseract
from pdf2image import convert_from_bytes

from app.services.extractor import (
    clean_ocr_text,
    fast_preprocess,
    page_pixmap_to_image,
    DPI,
    TESSERACT_CONFIG,
    MIN_TEXT_LENGTH,
)

logger = logging.getLogger(__name__)

def process_page_and_search(page_num: int, pdf_bytes: bytes, keywords: List[str],
                           return_only_filtered: bool, dpi: int = DPI) -> Union[Dict, None]:
    """
    Process a single page: extract text using PyMuPDF (for digital PDFs), or OCR if needed.
    Returns a result dict or None for this page.
    """
    try:
        # Each thread must open its own fitz.Document for thread safety
        with fitz.open("pdf", pdf_bytes) as doc:
            page = doc.load_page(page_num)
            text = page.get_text()
            if len(text.strip()) < MIN_TEXT_LENGTH:
                # OCR fallback for scanned/image page
                pix = page.get_pixmap(dpi=dpi)
                img = page_pixmap_to_image(pix)
                img = fast_preprocess(img)
                text = pytesseract.image_to_string(img, config=TESSERACT_CONFIG)
                # 1. Render the specific page as an image
                # images = convert_from_bytes(
                #     pdf_bytes, dpi=dpi, first_page=page_num + 1, last_page=page_num + 1
                # )
                # image = images[0]
                #
                # # 2. Convert PIL image to OpenCV format
                # img_byte_arr = io.BytesIO()
                # image.save(img_byte_arr, format='PNG')
                # img_array = np.frombuffer(img_byte_arr.getvalue(), np.uint8)
                # img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
                #
                # # 3. Do OCR
                # text = pytesseract.image_to_string(img, config=TESSERACT_CONFIG)
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
    pdf_path: str,
    keywords: List[str],
    return_only_filtered: bool = False,
    THREADS: int = 8
) -> Dict[str, Union[List[Dict], Dict]]:
    """
    Extract and search PDF pages in parallel using threads.
    Returns structured search response.
    """
    results = []
    try:
        # Read PDF file into memory ONCE for efficiency/thread safety.
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()

        # Open DOCUMENT ONCE (not for thread sharing), just to get number of pages.
        with fitz.open("pdf", pdf_bytes) as doc:
            num_pages = len(doc)

        # Recommended: use ThreadPoolExecutor WITH context manager [2][4]
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = [
                executor.submit(
                    process_page_and_search,
                    i, pdf_bytes, keywords, return_only_filtered
                ) for i in range(num_pages)
            ]

            for future in futures:
                result = future.result()
                if result:
                    results.append(result)

        # Handle not found and error cases for consistency
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
        logger.error(f"Error in parallel keyword processor: {e}")
        return {
            "imageToTextSearchResponse": {
                "keywordMatched": False,
                "selectedKeywords": "ERROR",
                "pageContent": str(e)
            }
        }
