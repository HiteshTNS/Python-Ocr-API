import os
import json
import logging
import pytesseract
import fitz  # PyMuPDF
from concurrent.futures import ThreadPoolExecutor, as_completed
import cv2
import numpy as np
import tempfile

from app.models.config import AppSettings
from app.utils.s3_utils import list_pdfs_in_s3, download_s3_file, upload_s3_json

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

# Configuration
DPI = 100
THREADS = 16
TESSERACT_CONFIG = '--oem 1 --psm 6 -c preserve_interword_spaces=1'
MIN_TEXT_LENGTH = 50

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

def pdf_to_images(pdf_path):
    try:
        doc = fitz.open(pdf_path)
        return [doc.load_page(i).get_pixmap(dpi=DPI) for i in range(len(doc))]
    except Exception as e:
        logger.error("Rendering failed for %s: %s", pdf_path, e)
        return []

def process_page(pix):
    try:
        img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
        img = fast_preprocess(img)
        return pytesseract.image_to_string(img, config=TESSERACT_CONFIG)
    except Exception as e:
        logger.error("OCR failed: %s", e)
        return ""

def extract_text_from_pdf(pdf_path):
    try:
        if is_digital_pdf(pdf_path):
            logger.info(f"Processing digital PDF: {pdf_path}")
            with fitz.open(pdf_path) as doc:
                return [page.get_text() for page in doc]
        else:
            logger.info(f"Processing scanned PDF with OCR: {pdf_path}")
            pixmaps = pdf_to_images(pdf_path)
            texts = [process_page(pix) for pix in pixmaps]
            return texts
    except Exception as e:
        logger.error("Failed to extract text from %s: %s", pdf_path, e)
        return []

def process_single_pdf(args):
    s3_key, dpi = args
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=True) as tmp_pdf:
        download_s3_file(s3_key, tmp_pdf.name)
        try:
            logger.info("Processing: %s", s3_key)
            text = extract_text_from_pdf(tmp_pdf.name)
            return os.path.basename(s3_key), text
        except Exception as e:
            logger.error("Failed to process %s: %s", s3_key, e)
            return os.path.basename(s3_key), None

def process_folder_fast_s3(batch_size, dpi=100):
    pdf_keys = list_pdfs_in_s3()
    total_files = len(pdf_keys)
    processed_count = 0
    batch_number = 1
    max_workers = min(os.cpu_count() or 4, batch_size, THREADS)
    logger.info("Detected CPU count: %d", os.cpu_count())
    logger.info("Max workers being used: %d", max_workers)
    logger.info("Batch size: %d", batch_size)

    for i in range(0, total_files, batch_size):
        batch = pdf_keys[i:i + batch_size]
        args_list = [(key, dpi) for key in batch]
        batch_result = {}
        logger.info("Starting Batch %d: Files %d to %d", batch_number, i + 1, min(i + batch_size, total_files))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_single_pdf, args): args[0] for args in args_list}
            for future in as_completed(futures):
                filename, text = future.result()
                if text is not None:
                    batch_result[filename] = text
                    processed_count += 1
        # Save after each batch to a unique file (locally, then upload to S3)
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False, mode='w', encoding='utf-8') as tmp_json:
            json.dump(batch_result, tmp_json, indent=2, ensure_ascii=False)
            tmp_json_path = tmp_json.name
        s3_json_key = os.path.join(settings.pdf_json_output_prefix, f"ExtractedData_Batch{batch_number}.json")
        upload_s3_json(tmp_json_path, s3_json_key)
        os.remove(tmp_json_path)
        logger.info("Batch %d processed and saved to S3: %s", batch_number, s3_json_key)
        batch_number += 1

    logger.info("Extraction complete. %d files processed in %d batches.", processed_count, batch_number - 1)
    return processed_count, total_files

def process_all_pdfs():
    processed_count, total_files = process_folder_fast_s3(settings.batch_size)
    if total_files == 0:
        return False, settings.pdf_json_output_prefix, "", 0, 0
    percent = processed_count / total_files
    return percent >= 0.9, settings.pdf_json_output_prefix, "", processed_count, total_files

# Example usage (for testing, not for FastAPI runtime)

