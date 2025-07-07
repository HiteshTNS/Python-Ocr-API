import os
import json
import re
from typing import Optional, List, Dict
from difflib import SequenceMatcher

from app.Exception.NoMatchFoundException import NoMatchFoundException

DATA_FOLDER = r"C:\Users\hitesh.paliwal\Desktop\Python Ocr"
JSON_FILE = os.path.join(DATA_FOLDER, "data.json")

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
    search_params: Dict[str, Optional[str]]
) -> List[str]:
    """
    Priority:
    1. If Dealer/Contract/Claim provided, search for all (AND logic). If found, return.
    2. If not found and VIN is provided, search for closest VIN match (with OCR normalization).
    3. If only VIN, Invoice_Date, or searchbyany is provided, search for those.
    """
    if not os.path.exists(JSON_FILE):
        print(f"JSON file not found at: {JSON_FILE}")
        return []

    with open(JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    fielded_params = {
        k: v.strip() for k, v in search_params.items()
        if k in AND_FIELDS and v and v.strip()
    }
    vin_param = search_params.get("VIN", "").strip()
    invoice_date_param = search_params.get("Invoice_Date", "").strip()
    searchbyany_param = search_params.get("searchbyany", "").strip()

    # 1. Dealer/Contract/Claim block (AND logic)
    if fielded_params:
        matching_files = []
        for filename, pages in data.items():
            if isinstance(pages, list):
                all_text = "\n".join(pages)
            else:
                all_text = str(pages)
            all_fields_match = True
            for field, value in fielded_params.items():
                if field in ["Claim", "Contract"]:
                    extracted_numbers = extract_numeric_after_keyword(all_text, field, min_digits=6)
                    print(f"[{filename}] Field: {field}, Expected: '{value}', Extracted numbers: {extracted_numbers}")
                    found = False
                    for num in extracted_numbers:
                        print(f"[{filename}] Comparing extracted '{num.strip()}' with expected '{value.strip()}'")
                        if num.strip() == value.strip():
                            found = True
                            print(f"[{filename}] {field} found: '{value}'")
                            break
                    if found:
                        continue
                    else:
                        all_fields_match = False
                        print(f"[{filename}]  --> No match for {field} ('{value}')")
                        break
                elif field == "Dealer":
                    pattern = re.compile(FIELD_PATTERNS[field], re.IGNORECASE)
                    field_match = False
                    for match in pattern.finditer(all_text):
                        extracted_value = match.group(1).strip().rstrip(':;\\').strip()
                        extracted_value_clean = re.sub(r'\s*\d+\s*$', '', extracted_value)
                        print(f"[{filename}] Field: {field}, Expected: '{value}', Extracted: '{extracted_value_clean}'")
                        if value.lower() in extracted_value_clean.lower():
                            field_match = True
                            break
                    if not field_match:
                        all_fields_match = False
                        print(f"[{filename}]  --> No match for {field} ('{value}')")
                        break
            if all_fields_match:
                print(f"[{filename}]  --> ALL required fields matched")
                matching_files.append(filename)
        if matching_files:
            return matching_files
        # Fallback to VIN search if VIN is provided and no AND match found
        elif vin_param:
            print("No match for Dealer/Contract/Claim. Trying VIN fallback...")
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
                print(f"[{filename}] Normalized search VIN: {vin_param_normalized}")
                print(f"[{filename}] Normalized candidate VINs: {vin_candidates}")
                if vin_param_normalized in vin_candidates:
                    print(f"[{filename}] VIN found: '{vin_param}'")
                    return [filename]
                else:
                    match = get_best_fuzzy_match(vin_param_normalized, vin_candidates, threshold=0.6)
                    if match:
                        ratio = SequenceMatcher(None, vin_param_normalized, match).ratio()
                        print(f"[{filename}]  --> Closest VIN match: {match} (score: {ratio:.2f})")
                        if ratio > best_ratio:
                            best_ratio = ratio
                            best_match = match
                            best_file = filename
            if best_file:
                print(f"[{best_file}]  --> Returning closest VIN match: {best_match}")
                return [best_file]
            else:
                raise NoMatchFoundException(f"VIN '{vin_param}' not found.")
        else:
            raise NoMatchFoundException(str(search_params))

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
            print(f"[{filename}] Normalized search VIN: {vin_param_normalized}")
            print(f"[{filename}] Normalized candidate VINs: {vin_candidates}")
            if vin_param_normalized in vin_candidates:
                print(f"[{filename}] VIN found: '{vin_param}'")
                return [filename]
            else:
                match = get_best_fuzzy_match(vin_param_normalized, vin_candidates, threshold=0.6)
                if match:
                    ratio = SequenceMatcher(None, vin_param_normalized, match).ratio()
                    print(f"[{filename}]  --> Closest VIN match: {match} (score: {ratio:.2f})")
                    if ratio > best_ratio:
                        best_ratio = ratio
                        best_match = match
                        best_file = filename
        if best_file:
            print(f"[{best_file}]  --> Returning closest VIN match: {best_match}")
            return [best_file]
        else:
            raise NoMatchFoundException(f"VIN '{vin_param}' not found.")

    # 3. If only Invoice_Date is provided
    elif invoice_date_param and not (vin_param or searchbyany_param):
        matching_files = []
        for filename, pages in data.items():
            if isinstance(pages, list):
                all_text = "\n".join(pages)
            else:
                all_text = str(pages)
            if invoice_date_param in all_text:
                print(f"[{filename}] Invoice_Date found: '{invoice_date_param}'")
                matching_files.append(filename)
        if not matching_files:
            raise NoMatchFoundException(f"Invoice_Date '{invoice_date_param}' not found.")
        return matching_files

    # 4. If only searchbyany is provided
    elif searchbyany_param and not (vin_param or invoice_date_param):
        matching_files = []
        for filename, pages in data.items():
            if isinstance(pages, list):
                all_text = "\n".join(pages)
            else:
                all_text = str(pages)
            if searchbyany_param in all_text:
                print(f"[{filename}] searchbyany found: '{searchbyany_param}'")
                matching_files.append(filename)
        if not matching_files:
            raise NoMatchFoundException(f"searchbyany '{searchbyany_param}' not found.")
        return matching_files

    else:
        raise NoMatchFoundException("No valid search fields provided.")

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
    try:
        matching_files = search_claim_documents(search_params)
        print("Matching files:", matching_files)
    except NoMatchFoundException as e:
        print("No matching files found:", e)
