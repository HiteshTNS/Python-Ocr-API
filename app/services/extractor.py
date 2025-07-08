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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(processName)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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
    filename, folder_path, dpi = args
    pdf_path = os.path.join(folder_path, filename)
    try:
        logger.info("Processing: %s", filename)
        text = extract_text_from_pdf(pdf_path, dpi=dpi)
        return filename, text
    except Exception as e:
        logger.error("Failed to process %s: %s", filename, e)
        return filename, None

def process_folder_fast(folder_path, output_json_base, batch_size,dpi=100):
    pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]
    total_files = len(pdf_files)
    processed_count = 0
    batch_number = 1
    max_workers = min(os.cpu_count() or 4, batch_size)
    logger.info("Detected CPU count: %d", os.cpu_count())
    logger.info("Max workers being used: %d", max_workers)
    logger.info("Batch size: %d", batch_size)

    for i in range(0, total_files, batch_size):
        batch = pdf_files[i:i+batch_size]
        args_list = [(filename, folder_path, dpi) for filename in batch]
        batch_result = {}
        logger.info("Starting Batch %d: Files %d to %d", batch_number, i+1, min(i+batch_size, total_files))
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_single_pdf, args): args[0] for args in args_list}
            for future in as_completed(futures):
                filename, text = future.result()
                if text is not None:
                    batch_result[filename] = text
                    processed_count += 1
        # Save after each batch to a unique file
        output_dir = os.path.dirname(output_json_base)
        os.makedirs(output_dir, exist_ok=True)
        batch_json_path = os.path.join(output_dir, f"ExtractedData_Batch{batch_number}.json")
        with open(batch_json_path, 'w', encoding='utf-8') as f:
            json.dump(batch_result, f, indent=2, ensure_ascii=False)
        logger.info("Batch %d processed and saved to %s", batch_number, batch_json_path)
        batch_number += 1

    logger.info("Extraction complete. %d files processed in %d batches.", processed_count, batch_number-1)
    return processed_count, total_files

def process_all_pdfs(folder_path: str, output_json_base: str, batch_size):
    processed_count, total_files = process_folder_fast(folder_path, output_json_base, batch_size=batch_size)
    if total_files == 0:
        return False, output_json_base, ""
    percent = processed_count / total_files
    return percent >= 0.9, output_json_base, ""

# Example usage
if __name__ == "__main__":
    folder_path = r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF"
    output_json_base = r"C:\Users\hitesh.paliwal\Desktop\ExtractedData"
    batch_size = 100  # Set your desired batch size here
    success, json_file, _ = process_all_pdfs(folder_path, output_json_base, batch_size=batch_size)
    print("Success:", success)
    print("Base JSON path:", json_file)
