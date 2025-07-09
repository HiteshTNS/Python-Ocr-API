import os
import json
import re
import logging
import tempfile
from typing import Optional, List, Dict
from difflib import SequenceMatcher
from app.Exception.NoMatchFoundException import NoMatchFoundException
from app.models.config import AppSettings
from app.utils.s3_utils import (
    get_s3_client,
    list_jsons_in_s3,
    download_s3_file,
    copy_s3_file,
    list_files_in_s3_prefix, clear_s3_prefix,
)

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

def search_claim_documents(
    search_params: Dict[str, Optional[str]],
    settings: AppSettings
) -> List[str]:
    """
    Search for claim documents in S3 based on search_params.
    Downloads JSONs from S3, finds matches, and copies matching PDFs from source to destination S3 bucket.
    Returns the list of matching PDF filenames.
    """
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
    s3 = get_s3_client()
    clear_s3_prefix(
        s3,
        bucket=settings.s3_destination_bucket,
        prefix=""  # or a prefix if you use subfolders
    )
    logger.info(f"Cleared all files in destination bucket: {settings.s3_destination_bucket}")

    active_fields = {field_map[k]: v.strip() for k, v in search_params.items() if v and k in field_map}
    if not active_fields:
        raise NoMatchFoundException("No valid search fields provided.")

    s3 = get_s3_client()
    # List all JSON files in the S3 output prefix
    json_keys = list_jsons_in_s3(
        s3,
        bucket=settings.s3_source_bucket,
        prefix=settings.pdf_json_output_prefix
    )
    logger.info(f"Found {len(json_keys)} JSON files in {settings.s3_source_bucket}/{settings.pdf_json_output_prefix}")

    if not json_keys:
        raise FileNotFoundError(f"No JSON files found in: {settings.s3_source_bucket}/{settings.pdf_json_output_prefix}")

    matching_files = set()

    for json_idx, json_key in enumerate(json_keys, 1):
        logger.info(f"Searching in JSON file {json_idx}/{len(json_keys)}: {json_key}")
        # Download JSON to temp file
        with tempfile.NamedTemporaryFile(suffix=".json", delete=True) as tmp_json:
            download_s3_file(json_key, tmp_json.name)
            try:
                with open(tmp_json.name, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                logger.error(f"Could not load {json_key}: {e}")
                continue

            for file_idx, (filename, pages) in enumerate(data.items(), 1):
                logger.debug(f"Searching in file {filename} from {json_key} (file {file_idx})")
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
                            match = get_best_fuzzy_match(vin_param_normalized, vin_candidates, threshold=0.6)
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

    logger.info(f"Matching files to copy: {matching_files}")

    if not matching_files:
        provided = {k: v for k, v in search_params.items() if v}
        logger.warning(f"No value matching with the keyword: {provided}")
        raise NoMatchFoundException(f"No value matching with the keyword: {provided}")

    # Copy matching files from source S3 bucket to destination S3 bucket
    for filename in matching_files:
        if filename.startswith(settings.pdf_input_prefix):
            source_key = filename
        else:
            source_key = os.path.join(settings.pdf_input_prefix, filename)
        dest_key = os.path.basename(filename)  # Flat structure in destination bucket
        logger.info(f"Attempting to copy {source_key} from {settings.s3_source_bucket} to {dest_key} in {settings.s3_destination_bucket}")
        try:
            copy_s3_file(
                s3,
                source_bucket=settings.s3_source_bucket,
                source_key=source_key,
                dest_bucket=settings.s3_destination_bucket,
                dest_key=dest_key
            )
            logger.info(f"Successfully copied {source_key} to {settings.s3_destination_bucket}/{dest_key}")
        except Exception as e:
            logger.error(f"Failed to copy {source_key} to {settings.s3_destination_bucket}/{dest_key}: {e}")

    # List files in destination bucket after copy
    dest_files = list_files_in_s3_prefix(
        s3,
        bucket=settings.s3_destination_bucket,
        prefix=""
    )
    logger.info(f"Files in destination bucket after copy: {dest_files}")

    logger.info(f"Total matching files: {len(matching_files)}")
    return list(matching_files)
