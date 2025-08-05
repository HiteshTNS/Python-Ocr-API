import logging
import re
import os
from typing import List, Dict, Optional, Union
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

def rotate_image(img: np.ndarray, angle: int) -> np.ndarray:
    if angle == 90:
        return cv2.rotate(img, cv2.ROTATE_90_CLOCKWISE)
    elif angle == 180:
        return cv2.rotate(img, cv2.ROTATE_180)
    elif angle == 270:
        return cv2.rotate(img, cv2.ROTATE_90_COUNTERCLOCKWISE)
    return img  # No rotation or unsupported

def process_pdf_page(
    page_num: int,
    page: fitz.Page,
    keywords: List[str],
    return_only_filtered: bool
) -> Optional[Dict]:
    """
    Process a single PDF page: extract text or run OCR if text is too short,
    then match keywords and return structured result or None.
    """
    try:
        print("Inside Pdf Extractor")
        text = page.get_text()
        if len(text.strip()) < MIN_TEXT_LENGTH:
            pix = page.get_pixmap(dpi=DPI)
            img = page_pixmap_to_image(pix)
            rotation_angle = detect_rotation(img)
            if rotation_angle != 0:
                logger.info(f"[INFO] Rotating page {page_num + 1} by {rotation_angle} degrees")
                img = rotate_image(img, rotation_angle)
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
        elif return_only_filtered:
            # Explicit return if filtering and no keywords matched for this page
            return None

    except Exception as e:
        logger.error(f"[PDF Page {page_num}] Error: {e}")
    return None


def process_image_bytes(
    img_bytes: bytes,
    keywords: List[str],
    return_only_filtered: bool
) -> Dict[str, Union[List[Dict], Dict]]:
    """
    Process an image file (jpeg/png/tiff) given as bytes,
    run OCR after rotation correction & preprocessing,
    return structured result JSON matching PDF output format.
    """
    try:
        print("Inside Image Extractor")
        nparr = np.frombuffer(img_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None:
            raise ValueError("Unable to decode image file.")

        rotation_angle = detect_rotation(img)
        if rotation_angle != 0:
            img = rotate_image(img, rotation_angle)

        img = fast_preprocess(img)
        text = pytesseract.image_to_string(img, config=TESSERACT_CONFIG)

        cleaned = clean_ocr_text(text)
        matched_keywords = [
            kw for kw in keywords if re.search(rf'\b{re.escape(kw)}\b', cleaned, flags=re.IGNORECASE)
        ]

        if matched_keywords or not return_only_filtered:
            return {
                "imageToTextSearchResponse": [{
                    "pageNO": 1,
                    "keywordMatched": bool(matched_keywords),
                    "selectedKeywords": "|".join(matched_keywords),
                    "pageContent": cleaned.replace("\n", " ")
                }]
            }
        elif return_only_filtered:
            return {
                "imageToTextSearchResponse": [{
                    "pageNO": 0,
                    "keywordMatched": False,
                    "selectedKeywords": "NOT FOUND",
                    "pageContent": "null"
                }]
            }

        # Fallback explicit return - no matches and return_only_filtered==False
        return {
            "imageToTextSearchResponse": [{
                "pageNO": 0,
                "keywordMatched": False,
                "selectedKeywords": "",
                "pageContent": ""
            }]
        }

    except Exception as e:
        logger.exception("Image OCR failed!")
        return {
            "imageToTextSearchResponse": [{
                "pageNO": 0,
                "keywordMatched": False,
                "selectedKeywords": "ERROR",
                "pageContent": str(e)
            }]
        }


def search_keywords_live_parallel(
    pdf_bytes: bytes,
    mime_type: str,
    keywords: List[str],
    return_only_filtered: bool = False,
    THREADS: int = THREADS
) -> Dict[str, Union[List[Dict], Dict]]:
    """
    OCR search dispatcher supporting both PDFs and images.
    Processes PDFs page-wise in parallel, images as single page.
    Returns JSON structure with "imageToTextSearchResponse".
    """
    try:
        if mime_type == "application/pdf":
            results = []
            with fitz.open("pdf", pdf_bytes) as doc:
                pages = [doc.load_page(i) for i in range(len(doc))]
                logger.info(f"Loaded {len(pages)} pages; ThreadPoolExecutor with {THREADS} threads")

                with ThreadPoolExecutor(max_workers=THREADS) as executor:
                    futures = [
                        executor.submit(
                            process_pdf_page,
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

        elif mime_type in {"image/jpeg", "image/png", "image/jpg", "image/tiff"}:
            return process_image_bytes(pdf_bytes, keywords, return_only_filtered)

        else:
            logger.error(f"Unsupported MIME type: {mime_type}")
            return {
                "imageToTextSearchResponse": [{
                    "pageNO": 0,
                    "keywordMatched": False,
                    "selectedKeywords": "ERROR",
                    "pageContent": f"Unsupported MIME type: {mime_type}"
                }]
            }
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