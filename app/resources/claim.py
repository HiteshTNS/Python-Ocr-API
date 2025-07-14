from fastapi import APIRouter, HTTPException
import tempfile
import os
import logging

from app.models.config import AppSettings
from app.services.extractor import extract_text_from_pdf
from app.services.search import search_keywords_in_pdf
from app.utils.s3_utils import download_s3_file
from app.models.OCRSearchRequest import OCRSearchRequest

env_profile = os.environ.get("APP_PROFILE", "uat")
env_file = f".env.{env_profile}"
settings = AppSettings(_env_file=env_file)

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/getDocumentwithOCRSearchPyMuPdf")
def get_document_with_ocr_search(request: OCRSearchRequest):
    file_id = request.file_Id
    keywords_str = request.keywords
    return_only_filtered = getattr(request, "returnOnlyFilteredPages", False)

    # Support both string and list for keywords
    if isinstance(keywords_str, str):
        keywords = [k.strip() for k in keywords_str.split("|") if k.strip()]
    else:
        keywords = keywords_str

    pdf_s3_key = f"{file_id}.pdf"

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_pdf:
        tmp_pdf_path = tmp_pdf.name

    try:
        try:
            download_s3_file(pdf_s3_key, tmp_pdf_path, settings=settings)
        except Exception as e:
            logger.error(f"Failed to download PDF from S3: {e}")
            raise HTTPException(status_code=404, detail=f"PDF not found: {pdf_s3_key}")

        all_page_text = extract_text_from_pdf(tmp_pdf_path)

        search_response = search_keywords_in_pdf(
            all_page_text, keywords, return_only_filtered
        )

        return search_response
    finally:
        # Remove the PDF from S3 and local temp file
        try:
            # delete_s3_file(pdf_s3_key, settings=settings)
            print("File deleted log")
        except Exception:
            pass
        try:
            os.remove(tmp_pdf_path)
        except Exception:
            pass
