import os
import logging
import pytesseract
import fitz  # PyMuPDF
import cv2
import numpy as np
import tempfile
import re
from concurrent.futures import ThreadPoolExecutor
import pdfplumber
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

def process_page(pix):
    try:
        img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
        img = fast_preprocess(img)
        text = pytesseract.image_to_string(img, config=TESSERACT_CONFIG)
        return clean_ocr_text(text)
    except Exception as e:
        logger.error("OCR failed: %s", e)
        return ""

def extract_text_from_pdf(pdf_path, THREADS=16):
    """
    Extract text from PDF using pdfplumber for digital or OCR for scanned.
    Returns a list of cleaned page texts.
    """
    try:
        if is_digital_pdf(pdf_path):
            logger.info(f"Processing digital PDF with pdfplumber: {pdf_path}")
            try:
                with pdfplumber.open(pdf_path) as pdf:
                    return [clean_ocr_text(page.extract_text() or "") for page in pdf.pages]
            except Exception as e:
                logger.warning(f"pdfplumber failed for {pdf_path}, falling back to fitz: {e}")
                with fitz.open(pdf_path) as doc:
                    return [clean_ocr_text(page.get_text()) for page in doc]
        else:
            logger.info(f"Processing scanned PDF with OCR: {pdf_path}")
            pixmaps = pdf_to_images(pdf_path)
            with ThreadPoolExecutor(max_workers=THREADS) as executor:
                texts = list(executor.map(process_page, pixmaps))
            return texts
    except Exception as e:
        logger.error("Failed to extract text from %s: %s", pdf_path, e)
        return []

def extract_pdf_text_from_s3(s3_key, THREADS=16):
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=True) as tmp_pdf:
        download_s3_file(s3_key, tmp_pdf.name, settings=settings)
        return extract_text_from_pdf(tmp_pdf.name, THREADS=THREADS)
