import os
import json
import re
import shutil
import logging
from typing import Optional, List, Dict
from difflib import SequenceMatcher
from app.Exception.NoMatchFoundException import NoMatchFoundException

VIN_MIN_LENGTH = 13

FIELD_PATTERNS = {
    "Dealer": r"dealer[:;\s#]*([^\n\r]+)",
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def ocr_vin_normalize(s: str) -> str:
    return (
        s.upper()
        .replace('O', '0')
        .replace('Q', '0')
        .replace('I', '1')
    )

def find_vin_candidates(text: str) -> List[str]:
    vin_candidates = []
    vin_lines = re.findall(r'VIN[:\s]*([A-Z0-9\W]{13,25})', text.upper())
    for raw in vin_lines:
        normalized = re.sub(r'[^A-HJ-NPR-Z0-9]', '', raw)
        if len(normalized) >= VIN_MIN_LENGTH:
            vin_candidates.append(normalized)
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

def clear_destination_folder(destination_folder: str):
    if os.path.exists(destination_folder):
        for filename in os.listdir(destination_folder):
            file_path = os.path.join(destination_folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                logger.error(f"Failed to delete {file_path}: {e}")
    else:
        os.makedirs(destination_folder, exist_ok=True)

def search_claim_documents(
    search_params: Dict[str, Optional[str]],
    input_folder: str,
    output_json_folder: str
) -> List[str]:
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

    active_fields = {field_map[k]: v.strip() for k, v in search_params.items() if v and k in field_map}
    if not active_fields:
        raise NoMatchFoundException("No valid search fields provided.")

    # Find all JSON files in the output_json_folder
    json_files = [os.path.join(output_json_folder, f)
                  for f in os.listdir(output_json_folder)
                  if f.lower().endswith('.json')]

    if not json_files:
        raise FileNotFoundError(f"No JSON files found in: {output_json_folder}")

    destination_folder = os.path.join(input_folder, "destination")
    clear_destination_folder(destination_folder)
    matching_files = set()

    for json_idx, json_file in enumerate(json_files, 1):
        logger.info(f"Searching in JSON file {json_idx}/{len(json_files)}: {json_file}")
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"Could not load {json_file}: {e}")
            continue

        for file_idx, (filename, pages) in enumerate(data.items(), 1):
            logger.debug(f"Searching in file {filename} from {json_file} (file {file_idx})")
            if isinstance(pages, list):
                all_text = "\n".join(pages)
            else:
                all_text = str(pages)

            for field, value in active_fields.items():
                found = False
                if field == "Contract":
                    extracted_numbers = extract_numeric_after_keyword(all_text, "Contract", min_digits=6)
                    if any(num.strip() == value for num in extracted_numbers):
                        found = True
                        logger.info(f"Match found for Contract in {filename}")
                elif field == "Claim":
                    extracted_numbers = extract_numeric_after_keyword(all_text, "Claim", min_digits=6)
                    if any(num.strip() == value for num in extracted_numbers):
                        found = True
                        logger.info(f"Match found for Claim in {filename}")
                elif field == "VIN":
                    vin_param_normalized = ocr_vin_normalize(re.sub(r'[^A-HJ-NPR-Z0-9]', '', value.upper()))
                    vin_candidates_raw = find_vin_candidates(all_text)
                    vin_candidates = [ocr_vin_normalize(v) for v in vin_candidates_raw]
                    if vin_param_normalized in vin_candidates:
                        found = True
                        logger.info(f"Exact VIN match in {filename}")
                    else:
                        match = get_best_fuzzy_match(vin_param_normalized, vin_candidates, threshold=0.6 )
                        if match:
                            found = True
                            logger.info(f"Fuzzy VIN match in {filename}")
                elif field == "Dealer":
                    pattern = re.compile(FIELD_PATTERNS["Dealer"], re.IGNORECASE)
                    for match in pattern.finditer(all_text):
                        extracted_value = match.group(1).strip().rstrip(':;\\').strip()
                        extracted_value_clean = re.sub(r'\s*\d+\s*$', '', extracted_value)
                        if value.lower() in extracted_value_clean.lower():
                            found = True
                            logger.info(f"Dealer match in {filename}")
                            break
                elif field == "searchbyany":
                    if value in all_text:
                        found = True
                        logger.info(f"Keyword '{value}' found in {filename}")
                if found:
                    matching_files.add(filename)
                    break  # No need to check other fields for this file

    if not matching_files:
        provided = {k: v for k, v in search_params.items() if v}
        logger.warning(f"No value matching with the keyword: {provided}")
        raise NoMatchFoundException(f"No value matching with the keyword: {provided}")

    # Copy matching files to destination subfolder inside input_folder
    for filename in matching_files:
        src_path = os.path.join(input_folder, filename)
        dst_path = os.path.join(destination_folder, filename)
        try:
            shutil.copy2(src_path, dst_path)
            logger.info(f"Copied {filename} to {destination_folder}")
        except Exception as e:
            logger.error(f"Failed to copy {filename}: {e}")

    logger.info(f"Total matching files: {len(matching_files)}")
    return list(matching_files)
