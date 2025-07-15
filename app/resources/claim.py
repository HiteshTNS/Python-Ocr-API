import time
from fastapi import APIRouter, HTTPException
import tempfile
import os
import logging

from app.models.config import AppSettings
from app.utils.s3_utils import download_s3_file, delete_s3_file
from app.models.OCRSearchRequest import OCRSearchRequest
from app.services.search import search_keywords_live_parallel

env_profile = os.environ.get("APP_PROFILE", "uat")
env_file = f".env.{env_profile}"
settings = AppSettings(_env_file=env_file)
enviornment = settings.enviornment
router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/getDocumentwithOCRSearchPyMuPdf")
def get_document_with_ocr_search(request: OCRSearchRequest):
    file_id = request.file_Id
    if file_id.lower().endswith('.pdf'):
        file_id = file_id[:-4]
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
            raise HTTPException(status_code=404, detail=f"PDF not found: {pdf_s3_key}")
        start_time = time.time()
        search_response = search_keywords_live_parallel(
            pdf_path=tmp_pdf_path,
            keywords=keywords,
            return_only_filtered=return_only_filtered,
            THREADS=16  # Or as needed
        )
        end_time = time.time()
        print(f"PDF processing and search took {end_time - start_time:.2f} seconds")
        return search_response
    finally:
        try:
            print("File deleted log")
        except Exception:
            pass
        try:
            os.remove(tmp_pdf_path)
        except Exception:
            pass
