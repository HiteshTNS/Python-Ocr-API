import os
import json
import pdfplumber
from pdf2image import convert_from_path
import pytesseract
from PIL import Image
import cv2
import numpy as np
import traceback

# Set Tesseract path if needed
pytesseract.pytesseract.tesseract_cmd = r'C:\Users\hitesh.paliwal\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'

def preprocess_image(pil_img):
    """Convert PIL Image to OpenCV, grayscale, and binarize for better OCR."""
    img = np.array(pil_img)
    if img.ndim == 3 and img.shape[2] == 4:
        img = cv2.cvtColor(img, cv2.COLOR_RGBA2RGB)
    gray = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
    thresh = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 2
    )
    return Image.fromarray(thresh)

def extract_text_from_pdf(pdf_path, dpi=300):
    """
    Extracts text from each page of a PDF.
    Tries pdfplumber first; falls back to OCR if no text found.
    Returns a list of text per page.
    """
    pages_text = []

    # Try digital extraction first
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
        traceback.print_exc()

    # Fallback to OCR for scanned PDFs
    try:
        images = convert_from_path(pdf_path, dpi=dpi)
        pages_text = []
        for img in images:
            pre_img = preprocess_image(img)
            ocr_text = pytesseract.image_to_string(pre_img, config="--psm 6")
            pages_text.append(ocr_text)
    except Exception as ocr_err:
        print(f"OCR failed for {pdf_path}: {ocr_err}")
        traceback.print_exc()

    return pages_text

def process_folder(folder_path, output_json_path, dpi=100):
    """
    Processes all PDFs in a folder and writes extracted text to a JSON file.
    JSON format: { "filename.pdf": ["page 1 text", "page 2 text", ...], ... }
    """
    result = {}
    pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]
    for filename in pdf_files:
        pdf_path = os.path.join(folder_path, filename)
        print(f"Processing: {filename}")
        result[filename] = extract_text_from_pdf(pdf_path, dpi=dpi)
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"Extraction complete. Output saved to {output_json_path}")

def process_all_pdfs(folder_path: str, output_json_path: str) -> str:
    """
    Wrapper function to process all PDFs in a folder and return the output JSON path.
    """
    process_folder(folder_path, output_json_path)
    return output_json_path

