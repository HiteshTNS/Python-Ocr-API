import logging
import re
import os
from typing import List, Dict, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import cv2
import fitz  # PyMuPDF
import pytesseract

# --- Configuration ---
DPI = 150
TESSERACT_CONFIG = '--oem 1 --psm 6'
MIN_TEXT_LENGTH = 40
THREADS = os.cpu_count() or 4
pytesseract.pytesseract.tesseract_cmd = r'C:\Users\st\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'
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
    img = arr.reshape((pix.h, pix.w, pix.n))
    if pix.n >= 4:
        img = cv2.cvtColor(img, cv2.COLOR_RGBA2RGB)
    return img
def detect_rotation(img: np.ndarray) -> int:
    try:
        osd = pytesseract.image_to_osd(img, config='--psm 0')
        for line in osd.splitlines():
            if "Rotate:" in line:
                angle = int(line.split(":")[-1].strip())
                return angle
    except Exception as e:
        logger.warning(f"[WARN] Rotation detection failed: {e}")
    return 0

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
             # Detect rotation and correct
            rotation_angle = detect_rotation(img)
            if rotation_angle != 0:
                logger.info(f"[INFO] Rotating page {page_num + 1} by {rotation_angle} degrees")
                img = cv2.rotate(img, {
                    90: cv2.ROTATE_90_CLOCKWISE,
                    180: cv2.ROTATE_180,
                    270: cv2.ROTATE_90_COUNTERCLOCKWISE
                }.get(rotation_angle, img))
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
    executor: ThreadPoolExecutor = None
) -> Dict[str, Union[List[Dict], Dict]]:
    results = []
    doc = None
    try:
        doc = fitz.open("pdf", pdf_bytes)
        pages = [doc.load_page(i) for i in range(len(doc))]
        logger.info(f"Loaded {len(pages)} pages; using ThreadPoolExecutor")

        if executor is None:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=THREADS) as local_executor:
                futures = [
                    local_executor.submit(
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
        else:
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

        # Sort results by page number for ordered output
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
