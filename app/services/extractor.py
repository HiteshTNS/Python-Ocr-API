import os
import json
import logging
import pdfplumber
from pdf2image import convert_from_path
import pytesseract
from PIL import Image
import cv2
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
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
    format="%(asctime)s - %(processName)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Tesseract path (make configurable if needed)
pytesseract.pytesseract.tesseract_cmd = r'C:\Users\hitesh.paliwal\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'

def preprocess_image(pil_img):
    img = np.array(pil_img)
    if img.ndim == 3 and img.shape[2] == 4:
        img = cv2.cvtColor(img, cv2.COLOR_RGBA2RGB)
    gray = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
    thresh = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 2
    )
    return Image.fromarray(thresh)

def extract_text_from_pdf(pdf_path, dpi=100):
    pages_text = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            any_text_found = False
            for page in pdf.pages:
                text = page.extract_text() or ""
                if text.strip():
                    any_text_found = True
                pages_text.append(text)
            if any_text_found:
                return pages_text
    except Exception as e:
        logger.error("pdfplumber failed for %s: %s", pdf_path, e)
    try:
        images = convert_from_path(pdf_path, dpi=dpi)
        pages_text = []
        for img in images:
            pre_img = preprocess_image(img)
            ocr_text = pytesseract.image_to_string(pre_img, config="--psm 6")
            pages_text.append(ocr_text)
    except Exception as ocr_err:
        logger.error("OCR failed for %s: %s", pdf_path, ocr_err)
    return pages_text

def process_single_pdf(args):
    key, dpi = args
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=True) as tmp_pdf:
        download_s3_file(key, tmp_pdf.name)
        try:
            logger.info("Processing: %s", key)
            text = extract_text_from_pdf(tmp_pdf.name, dpi=dpi)
            # return key, text
            return os.path.basename(key), text
        except Exception as e:
            logger.error("Failed to process %s: %s", key, e)
            # return key, None
            return os.path.basename(key), None

def process_folder_fast_s3(batch_size, dpi=100):
    pdf_keys = list_pdfs_in_s3()
    total_files = len(pdf_keys)
    processed_count = 0
    batch_number = 1
    max_workers = min(os.cpu_count() or 4, batch_size)
    logger.info("Detected CPU count: %d", os.cpu_count())
    logger.info("Max workers being used: %d", max_workers)
    logger.info("Batch size: %d", batch_size)

    for i in range(0, total_files, batch_size):
        batch = pdf_keys[i:i+batch_size]
        args_list = [(key, dpi) for key in batch]
        batch_result = {}
        logger.info("Starting Batch %d: Files %d to %d", batch_number, i+1, min(i+batch_size, total_files))
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
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

    logger.info("Extraction complete. %d files processed in %d batches.", processed_count, batch_number-1)
    return processed_count, total_files

def process_all_pdfs():
    processed_count, total_files = process_folder_fast_s3(settings.batch_size)
    if total_files == 0:
        return False, settings.pdf_json_output_prefix, "", 0, 0
    percent = processed_count / total_files
    return percent >= 0.9, settings.pdf_json_output_prefix, "", processed_count, total_files

# Example usage (for testing, not for FastAPI runtime)
if __name__ == "__main__":
    success, json_folder, _, processed_count, total_files = process_all_pdfs()
    print("Success:", success)
    print("JSON output S3 prefix:", json_folder)
    print("Processed:", processed_count, "Total:", total_files)
