import os
import json
import logging
import pytesseract
import fitz  # PyMuPDF
from concurrent.futures import ThreadPoolExecutor, as_completed
import cv2
import numpy as np
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(processName)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
DPI = 100  # Reduced DPI for faster processing
THREADS = 16 # Adjusted for optimal CPU usage
TESSERACT_CONFIG = '--oem 1 --psm 6 -c preserve_interword_spaces=1'
MIN_TEXT_LENGTH = 50  # Minimum characters to consider as digital PDF


def is_digital_pdf(pdf_path):
    """Check if PDF contains selectable text (digital PDF)"""
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
    """Preprocess image for OCR"""
    gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
    return cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]


def pdf_to_images(pdf_path):
    """Convert PDF pages to images (for scanned PDFs only)"""
    try:
        doc = fitz.open(pdf_path)
        return [doc.load_page(i).get_pixmap(dpi=DPI) for i in range(len(doc))]
    except Exception as e:
        logger.error("Rendering failed for %s: %s", pdf_path, e)
        return []


def process_page(pix):
    """OCR for a single page"""
    try:
        img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
        img = fast_preprocess(img)
        return pytesseract.image_to_string(img, config=TESSERACT_CONFIG)
    except Exception as e:
        logger.error("OCR failed: %s", e)
        return ""


def extract_text_from_pdf(pdf_path):
    """Extract text from PDF using appropriate method based on PDF type"""
    try:
        if is_digital_pdf(pdf_path):
            logger.info(f"Processing digital PDF: {pdf_path}")
            with fitz.open(pdf_path) as doc:
                return [page.get_text() for page in doc]
        else:
            logger.info(f"Processing scanned PDF with OCR: {pdf_path}")
            pixmaps = pdf_to_images(pdf_path)
            with ThreadPoolExecutor(max_workers=THREADS) as executor:
                texts = list(executor.map(process_page, pixmaps))
            return texts
    except Exception as e:
        logger.error("Failed to extract text from %s: %s", pdf_path, e)
        return []


def process_single_pdf(args):
    filename, folder_path = args
    pdf_path = os.path.join(folder_path, filename)
    try:
        logger.info("Processing: %s", filename)
        text = extract_text_from_pdf(pdf_path)
        return filename, text
    except Exception as e:
        logger.error("Failed to process %s: %s", filename, e)
        return filename, None


def process_folder_fast(folder_path, output_json_base, batch_size):
    pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]
    total_files = len(pdf_files)
    processed_count = 0
    batch_number = 1
    logger.info("Detected PDF files: %d", total_files)

    for i in range(0, total_files, batch_size):
        batch = pdf_files[i:i + batch_size]
        args_list = [(filename, folder_path) for filename in batch]
        batch_result = {}
        logger.info("Starting Batch %d: Files %d to %d", batch_number, i + 1, min(i + batch_size, total_files))

        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = {executor.submit(process_single_pdf, args): args[0] for args in args_list}
            for future in as_completed(futures):
                filename, text = future.result()
                if text is not None:
                    batch_result[filename] = text
                    processed_count += 1

        # Save after each batch to a unique file
        os.makedirs(output_json_base, exist_ok=True)
        batch_json_path = os.path.join(output_json_base, f"ExtractedData_Batch{batch_number}.json")
        with open(batch_json_path, 'w', encoding='utf-8') as f:
            json.dump(batch_result, f, indent=2, ensure_ascii=False)

        logger.info("Batch %d processed and saved to %s", batch_number, batch_json_path)
        batch_number += 1

    logger.info("Extraction complete. %d files processed in %d batches.", processed_count, batch_number - 1)
    return processed_count, total_files


def process_all_pdfs(folder_path: str, output_json_base: str, batch_size):
    processed_count, total_files = process_folder_fast(folder_path, output_json_base, batch_size=batch_size)
    if total_files == 0:
        return False, output_json_base, ""
    percent = processed_count / total_files
    return percent >= 0.9, output_json_base, ""