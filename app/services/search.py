import os
import json
import re
import shutil
from typing import Optional, List, Dict
from difflib import SequenceMatcher
from app.Exception.NoMatchFoundException import NoMatchFoundException

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
    # Map both new and old field names to internal logic
    field_map = {
        "Dealer Name": "Dealer",
        "Dealer": "Dealer",
        "VIN": "VIN",
        "Contract #": "Contract",
        "Contract": "Contract",
        "Claim #": "Claim",
        "Claim": "Claim",
        "Search by Word": "searchbyany",
        "searchbyany": "searchbyany"
    }

    # Extract only non-empty search fields
    active_fields = {field_map[k]: v.strip() for k, v in search_params.items() if v and k in field_map}

    if not active_fields:
        raise NoMatchFoundException("No valid search fields provided.")

    if not os.path.exists(json_file):
        raise FileNotFoundError(f"JSON file not found at: {json_file}")

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    matching_files = set()  # Use a set to avoid duplicates

    for filename, pages in data.items():
        if isinstance(pages, list):
            all_text = "\n".join(pages)
        else:
            all_text = str(pages)

        # For each file, check all active fields (OR logic)
        for field, value in active_fields.items():
            found = False
            if field == "Contract":
                extracted_numbers = extract_numeric_after_keyword(all_text, "Contract", min_digits=6)
                if any(num.strip() == value for num in extracted_numbers):
                    found = True
            elif field == "Claim":
                extracted_numbers = extract_numeric_after_keyword(all_text, "Claim", min_digits=6)
                if any(num.strip() == value for num in extracted_numbers):
                    found = True
            elif field == "VIN":
                vin_param_normalized = ocr_vin_normalize(re.sub(r'[^A-HJ-NPR-Z0-9]', '', value.upper()))
                vin_candidates_raw = find_vin_candidates(all_text)
                vin_candidates = [ocr_vin_normalize(v) for v in vin_candidates_raw]
                if vin_param_normalized in vin_candidates:
                    found = True
                else:
                    match = get_best_fuzzy_match(vin_param_normalized, vin_candidates, threshold=0.6)
                    if match:
                        found = True
            elif field == "Dealer":
                pattern = re.compile(FIELD_PATTERNS["Dealer"], re.IGNORECASE)
                for match in pattern.finditer(all_text):
                    extracted_value = match.group(1).strip().rstrip(':;\\').strip()
                    extracted_value_clean = re.sub(r'\s*\d+\s*$', '', extracted_value)
                    if value.lower() in extracted_value_clean.lower():
                        found = True
                        break
            elif field == "searchbyany":
                if value in all_text:
                    found = True
            if found:
                matching_files.add(filename)
                break  # No need to check other fields for this file

    if not matching_files:
        provided = {k: v for k, v in search_params.items() if v}
        raise NoMatchFoundException(f"No value matching with the keyword: {provided}")

    # Copy matching files to destination subfolder inside input_folder
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

    return list(matching_files)

# Example usage:
if __name__ == "__main__":
    search_params = {
        "Dealer Name": "",
        "VIN": "WAUGNAF46HN038604",
        "Contract #": "6001285079",
        "Claim #": "71699758",
        "Search by Word": ""
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
