import os
import json
import re
import shutil
from typing import Optional, List, Dict
from difflib import SequenceMatcher
import shutil
from app.Exception.NoMatchFoundException import NoMatchFoundException

AND_FIELDS = ["Dealer", "Contract", "Claim"]
VIN_MIN_LENGTH = 13

FIELD_PATTERNS = {
    "Dealer": r"dealer[:;\s#]*([^\n\r]+)",
}

def ocr_vin_normalize(s: str) -> str:
    """
    Replaces common OCR errors in VINs:
    - 'O' <-> '0'
    - 'I' <-> '1'
    - 'Q' <-> '0'
    """
    return (
        s.upper()
        .replace('O', '0')
        .replace('Q', '0')
        .replace('I', '1')
    )

def find_vin_candidates(text: str) -> List[str]:
    """
    Extract possible VINs (13+ chars, robust to OCR errors and non-alphanumeric separators).
    """
    vin_candidates = []
    # 1. Look for lines like 'VIN: ...'
    vin_lines = re.findall(r'VIN[:\s]*([A-Z0-9\W]{13,25})', text.upper())
    for raw in vin_lines:
        normalized = re.sub(r'[^A-HJ-NPR-Z0-9]', '', raw)
        if len(normalized) >= VIN_MIN_LENGTH:
            vin_candidates.append(normalized)
    # 2. General fallback for any other possible VINs in the text
    raw_candidates = re.findall(r'([A-HJ-NPR-Z0-9][A-HJ-NPR-Z0-9\W]{12,})', text.upper())
    for raw in raw_candidates:
        normalized = re.sub(r'[^A-HJ-NPR-Z0-9]', '', raw)
        if len(normalized) >= VIN_MIN_LENGTH and normalized not in vin_candidates:
            vin_candidates.append(normalized)
    return vin_candidates

def extract_numeric_after_keyword(text: str, keyword: str, min_digits: int = 6) -> List[str]:
    results = []
    lines = text.splitlines()
    keyword_lower = keyword.lower()
    for line in lines:
        if keyword_lower in line.lower():
            idx = line.lower().find(keyword_lower)
            after = line[idx + len(keyword):]
            numbers = re.findall(r'\d+', after)
            long_numbers = [num for num in numbers if len(num) >= min_digits]
            results.extend(long_numbers)
    return results

def get_best_fuzzy_match(target: str, candidates: List[str], threshold: float = 0.6) -> Optional[str]:
    best_ratio = 0
    best_candidate = None
    for cand in candidates:
        ratio = SequenceMatcher(None, target, cand).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_candidate = cand
    if best_ratio >= threshold:
        return best_candidate
    return None

def search_claim_documents(
    search_params: Dict[str, Optional[str]],
    input_folder: str,
    json_file: str
) -> List[str]:

    destination_folder = os.path.join(input_folder, "destination")
    # Clear the destination folder before copying new files
    if os.path.exists(destination_folder):
        for filename in os.listdir(destination_folder):
            file_path = os.path.join(destination_folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")
    else:
        os.makedirs(destination_folder, exist_ok=True)


    """
    Priority:
    1. If Dealer/Contract/Claim provided, search for all (AND logic). If found, return.
    2. If not found and VIN is provided, search for closest VIN match (with OCR normalization).
    3. If only VIN, Invoice_Date, or searchbyany is provided, search for those.
    Additionally, copy all matching files to 'destination' subfolder inside input_folder.
    """
    if not os.path.exists(json_file):
        raise FileNotFoundError(f"JSON file not found at: {json_file}")

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    fielded_params = {
        k: v.strip() for k, v in search_params.items()
        if k in AND_FIELDS and v and v.strip()
    }
    vin_param = search_params.get("VIN", "").strip()
    invoice_date_param = search_params.get("Invoice_Date", "").strip()
    searchbyany_param = search_params.get("searchbyany", "").strip()

    matching_files = []

    # 1. Dealer/Contract/Claim block (AND logic)
    if fielded_params:
        for filename, pages in data.items():
            if isinstance(pages, list):
                all_text = "\n".join(pages)
            else:
                all_text = str(pages)
            all_fields_match = True
            for field, value in fielded_params.items():
                if field in ["Claim", "Contract"]:
                    extracted_numbers = extract_numeric_after_keyword(all_text, field, min_digits=6)
                    found = False
                    for num in extracted_numbers:
                        if num.strip() == value.strip():
                            found = True
                            break
                    if not found:
                        all_fields_match = False
                        break
                elif field == "Dealer":
                    pattern = re.compile(FIELD_PATTERNS[field], re.IGNORECASE)
                    field_match = False
                    for match in pattern.finditer(all_text):
                        extracted_value = match.group(1).strip().rstrip(':;\\').strip()
                        extracted_value_clean = re.sub(r'\s*\d+\s*$', '', extracted_value)
                        if value.lower() in extracted_value_clean.lower():
                            field_match = True
                            break
                    if not field_match:
                        all_fields_match = False
                        break
            if all_fields_match:
                matching_files.append(filename)
        # Fallback to VIN search if VIN is provided and no AND match found
        if not matching_files and vin_param:
            vin_param_normalized = ocr_vin_normalize(re.sub(r'[^A-HJ-NPR-Z0-9]', '', vin_param.upper()))
            best_match = None
            best_file = None
            best_ratio = 0
            for filename, pages in data.items():
                if isinstance(pages, list):
                    all_text = "\n".join(pages)
                else:
                    all_text = str(pages)
                vin_candidates_raw = find_vin_candidates(all_text)
                vin_candidates = [ocr_vin_normalize(v) for v in vin_candidates_raw]
                if vin_param_normalized in vin_candidates:
                    matching_files = [filename]
                    break
                else:
                    match = get_best_fuzzy_match(vin_param_normalized, vin_candidates, threshold=0.6)
                    if match:
                        ratio = SequenceMatcher(None, vin_param_normalized, match).ratio()
                        if ratio > best_ratio:
                            best_ratio = ratio
                            best_match = match
                            best_file = filename
            if not matching_files and best_file:
                matching_files = [best_file]
        if not matching_files:
            provided = {k: v for k, v in search_params.items() if v}
            raise NoMatchFoundException(f"No value matching with the keyword: {provided}")


    # 2. If only VIN is provided
    elif vin_param and not (invoice_date_param or searchbyany_param):
        vin_param_normalized = ocr_vin_normalize(re.sub(r'[^A-HJ-NPR-Z0-9]', '', vin_param.upper()))
        best_match = None
        best_file = None
        best_ratio = 0
        for filename, pages in data.items():
            if isinstance(pages, list):
                all_text = "\n".join(pages)
            else:
                all_text = str(pages)
            vin_candidates_raw = find_vin_candidates(all_text)
            vin_candidates = [ocr_vin_normalize(v) for v in vin_candidates_raw]
            if vin_param_normalized in vin_candidates:
                matching_files = [filename]
                break
            else:
                match = get_best_fuzzy_match(vin_param_normalized, vin_candidates, threshold=0.6)
                if match:
                    ratio = SequenceMatcher(None, vin_param_normalized, match).ratio()
                    if ratio > best_ratio:
                        best_ratio = ratio
                        best_match = match
                        best_file = filename
        if not matching_files and best_file:
            matching_files = [best_file]
        if not matching_files:
            provided = {k: v for k, v in search_params.items() if v}
            raise NoMatchFoundException(f"No value matching with the keyword: {provided}")


    # 3. If only Invoice_Date is provided
    elif invoice_date_param and not (vin_param or searchbyany_param):
        for filename, pages in data.items():
            if isinstance(pages, list):
                all_text = "\n".join(pages)
            else:
                all_text = str(pages)
            if invoice_date_param in all_text:
                matching_files.append(filename)
        if not matching_files:
            provided = {k: v for k, v in search_params.items() if v}
            raise NoMatchFoundException(f"No value matching with the keyword: {provided}")


    # 4. If only searchbyany is provided
    elif searchbyany_param and not (vin_param or invoice_date_param):
        for filename, pages in data.items():
            if isinstance(pages, list):
                all_text = "\n".join(pages)
            else:
                all_text = str(pages)
            if searchbyany_param in all_text:
                matching_files.append(filename)
        if not matching_files:
            provided = {k: v for k, v in search_params.items() if v}
            raise NoMatchFoundException(f"No value matching with the keyword: {provided}")


    else:
        raise NoMatchFoundException("No valid search fields provided.")

    # --- Copy matching files to destination subfolder inside input_folder ---
    destination_folder = os.path.join(input_folder, "destination")
    os.makedirs(destination_folder, exist_ok=True)
    for filename in matching_files:
        src_path = os.path.join(input_folder, filename)
        dst_path = os.path.join(destination_folder, filename)
        try:
            shutil.copy2(src_path, dst_path)
            print(f"Copied {filename} to {destination_folder}")
        except Exception as e:
            print(f"Failed to copy {filename}: {e}")

    return matching_files

# Example usage:
if __name__ == "__main__":
    search_params = {
        "Dealer": "",
        "VIN": "WAUGNAF46HN038604",
        "Contract": "",
        "Claim": "",
        "Invoice_Date": "",
        "searchbyany": ""
    }
    input_folder = r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF"
    json_file = r"C:\Users\hitesh.paliwal\Desktop\claims_data.json"
    try:
        matching_files = search_claim_documents(
            search_params,
            input_folder=input_folder,
            json_file=json_file
        )
        print("Matching files:", matching_files)
    except NoMatchFoundException as e:
        print("No matching files found:", e)
