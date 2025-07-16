
import time
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
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

@router.post("/getDocumentwithOCRSearchPyMuPdf", status_code=200)
def get_document_with_ocr_search(request: OCRSearchRequest):
    file_id = request.file_Id
    if file_id.lower().endswith('.pdf'):
        file_id = file_id[:-4]
    keywords_str = request.keywords
    return_only_filtered = getattr(request, "returnOnlyFilteredPages", False)
    CPU_THREADS = os.cpu_count() or 4
    # print("Maximum Threads using : " , CPU_THREADS)
    # Check if keywords are missing
    if not keywords_str:
        raise HTTPException(status_code=400, detail="Keywords cannot be empty.")

    # Simulate 401 (unauthorized)
    if file_id.lower() == "unauthorized":
        raise HTTPException(status_code=401, detail="Unauthorized access")

    # Simulate 403 (forbidden)
    if file_id.lower() == "forbidden":
        raise HTTPException(status_code=403, detail="Access to this file is forbidden")

    # Normalize keywords
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
            THREADS=CPU_THREADS
        )

        end_time = time.time()
        logger.info(f"PDF processing and search took {end_time - start_time:.2f} seconds")
        return JSONResponse(status_code=status.HTTP_200_OK, content=search_response)
    except HTTPException:
        raise  # Re-raise if already handled
    except Exception as e:
        logger.error(f"Internal error during OCR search: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        # Remove the PDF from S3 and local temp file
        try:
            delete_s3_file(pdf_s3_key, settings=settings)
        except Exception:
            pass
        try:
            os.remove(tmp_pdf_path)
        except Exception as cleanup_err:
            logger.warning(f"Failed to delete temp file: {cleanup_err}")
