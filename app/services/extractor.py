import os
import logging
import pytesseract
import fitz  # PyMuPDF
import cv2
import numpy as np
import tempfile
import re
from concurrent.futures import ProcessPoolExecutor

from app.models.config import AppSettings
from app.utils.s3_utils import download_s3_file

# Load settings from resource file
env_profile = os.environ.get("APP_PROFILE", "uat")
env_file = f".env.{env_profile}"
settings = AppSettings(_env_file=env_file)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DPI = 200  # Lowered for speed, increase if needed
TESSERACT_CONFIG = '--oem 1 --psm 6 -c preserve_interword_spaces=1'
MIN_TEXT_LENGTH = 50

def clean_ocr_text(text: str) -> str:
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n+', '\n', text)
    text = re.sub(r'^[ \t]+|[ \t]+$', '', text, flags=re.MULTILINE)
    return text.strip()

def is_digital_pdf(pdf_path):
    try:
        with fitz.open(pdf_path) as doc:
            text = ""
            for page in doc:
                text += page.get_text()
                if len(text) > MIN_TEXT_LENGTH:
                    return True
            return False
    except Exception as e:
        logger.error(f"Error checking PDF type for {pdf_path}: {str(e)}")
        return False

def fast_preprocess(img_array):
    gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
    return cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]

def pdf_to_images(pdf_path, dpi=DPI):
    try:
        doc = fitz.open(pdf_path)
        return [doc.load_page(i).get_pixmap(dpi=dpi) for i in range(len(doc))]
    except Exception as e:
        logger.error("Rendering failed for %s: %s", pdf_path, e)
        return []

def pixmap_to_png_bytes(pix):
    return pix.tobytes("png")

def ocr_page_from_bytes(png_bytes):
    try:
        img_array = cv2.imdecode(np.frombuffer(png_bytes, np.uint8), cv2.IMREAD_COLOR)
        img = fast_preprocess(img_array)
        return pytesseract.image_to_string(img, config=TESSERACT_CONFIG)
    except Exception as e:
        logger.error("OCR failed: %s", e)
        return ""

def extract_text_from_pdf(pdf_path, dpi=DPI, processes=None):
    """
    Extracts text from a PDF, using OCR for scanned PDFs.
    Uses multiprocessing for OCR on scanned pages.
    """
    try:
        if is_digital_pdf(pdf_path):
            logger.info(f"Processing digital PDF: {pdf_path}")
            with fitz.open(pdf_path) as doc:
                return [clean_ocr_text(page.get_text()) for page in doc]
        else:
            logger.info(f"Processing scanned PDF with OCR: {pdf_path}")
            pixmaps = pdf_to_images(pdf_path, dpi=dpi)
            png_bytes_list = [pixmap_to_png_bytes(pix) for pix in pixmaps]
            with ProcessPoolExecutor(max_workers=processes) as executor:
                texts = list(executor.map(ocr_page_from_bytes, png_bytes_list))
            cleaned_texts = [clean_ocr_text(page) for page in texts]
            return cleaned_texts
    except Exception as e:
        logger.error("Failed to extract text from %s: %s", pdf_path, e)
        return []

def extract_pdf_text_from_s3(s3_key, dpi=DPI, processes=None):
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=True) as tmp_pdf:
        download_s3_file(s3_key, tmp_pdf.name, settings=settings)
        return extract_text_from_pdf(tmp_pdf.name, dpi=dpi, processes=processes)

# Example usage:
if __name__ == "__main__":
    # For testing only, not for FastAPI runtime
    s3_key = "your_prefix/yourfile.pdf"
    texts = extract_pdf_text_from_s3(s3_key, dpi=200, processes=4)
    for i, page in enumerate(texts, 1):
        print(f"--- Page {i} ---\n{page}\n")
