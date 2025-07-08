import os
import json
import pdfplumber
from pdf2image import convert_from_path
import pytesseract
from PIL import Image
import cv2
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed

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
        print(f"pdfplumber failed for {pdf_path}: {e}")
    try:
        images = convert_from_path(pdf_path, dpi=dpi)
        pages_text = []
        for img in images:
            pre_img = preprocess_image(img)
            ocr_text = pytesseract.image_to_string(pre_img, config="--psm 6")
            pages_text.append(ocr_text)
    except Exception as ocr_err:
        print(f"OCR failed for {pdf_path}: {ocr_err}")
    return pages_text

def process_single_pdf(args):
    filename, folder_path, dpi = args
    pdf_path = os.path.join(folder_path, filename)
    try:
        print(f"Processing: {filename}")
        text = extract_text_from_pdf(pdf_path, dpi=dpi)
        return filename, text
    except Exception as e:
        print(f"Failed to process {filename}: {e}")
        return filename, None

def process_folder_fast(folder_path, output_json_path,batch_size,dpi=100):
    pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]
    total_files = len(pdf_files)
    result = {}
    processed_count = 0
    max_workers = min(os.cpu_count() or 4, batch_size)
    print("Detected CPU count:", os.cpu_count())
    print("Max workers being used:", max_workers)
    print("Batch size:", batch_size)
    for i in range(0, total_files, batch_size):
        batch = pdf_files[i:i+batch_size]
        args_list = [(filename, folder_path, dpi) for filename in batch]
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_single_pdf, args): args[0] for args in args_list}
            for future in as_completed(futures):
                filename, text = future.result()
                if text is not None:
                    result[filename] = text
                    processed_count += 1
        # Save after each batch
        output_dir = os.path.dirname(output_json_path)
        os.makedirs(output_dir, exist_ok=True)
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"Batch {i//batch_size + 1} processed and saved.")
    print(f"Extraction complete. Output saved to {output_json_path}")
    return processed_count, total_files

def process_all_pdfs(folder_path: str, output_json_path: str, batch_size):
    processed_count, total_files = process_folder_fast(folder_path, output_json_path, batch_size)
    if total_files == 0:
        return False, output_json_path, ""
    percent = processed_count / total_files
    return percent >= 0.9, output_json_path, ""

# Example usage
if __name__ == "__main__":
    folder_path = r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF"
    output_json = r"C:\Users\hitesh.paliwal\Desktop\claims_data.json"
    batch_size = 100  # Set your desired batch size here
    success, json_file = process_all_pdfs(folder_path, output_json, batch_size=batch_size)
    print("Success:", success)
    print("JSON file:", json_file)
