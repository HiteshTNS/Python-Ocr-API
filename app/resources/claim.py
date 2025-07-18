import os
import time
import logging
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
from app.models.config import AppSettings
from app.models.OCRSearchRequest import OCRSearchRequest
from app.services.search import search_keywords_live_parallel
from app.resources.sgresource import fetch_pdf_base64, save_base64_to_pdf  # Import your resource functions
import tempfile

env_profile = os.environ.get("APP_PROFILE", "uat")
env_file = f".env.{env_profile}"
settings = AppSettings(_env_file=env_file)
environment = settings.enviornment
router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/getDocumentwithOCRSearchPyMuPdf", status_code=200)
def get_document_with_ocr_search(request: OCRSearchRequest):
    file_id = request.file_Id
    keywords_str = request.keywords
    return_only_filtered = getattr(request, "returnOnlyFilteredPages", False)
    CPU_THREADS = os.cpu_count() or 4

    # Validate keywords
    if not keywords_str:
        raise HTTPException(status_code=400, detail="Keywords cannot be empty.")

    # Authorization Simulation
    if file_id.lower() == "unauthorized":
        raise HTTPException(status_code=401, detail="Unauthorized access")
    if file_id.lower() == "forbidden":
        raise HTTPException(status_code=403, detail="Access to this file is forbidden")

    # Normalize keywords (split by '|' if string)
    if isinstance(keywords_str, str):
        keywords = [k.strip() for k in keywords_str.split("|") if k.strip()]
    else:
        keywords = keywords_str

    # Fetch base64 PDF from internal API and save to temp file
    try:
        pdf_base64 = fetch_pdf_base64(file_id)
        tmp_pdf_path = save_base64_to_pdf(pdf_base64)
    except Exception as e:
        logger.error(f"Failed to fetch or decode PDF for file_id {file_id}: {e}")
        raise HTTPException(status_code=404, detail=f"Unable to fetch PDF for file_id {file_id}")

    try:
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
        raise
    except Exception as e:
        logger.error(f"Internal error during OCR search: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        # Always cleanup temp pdf file
        try:
            os.remove(tmp_pdf_path)
        except Exception as cleanup_err:
            logger.warning(f"Failed to delete temp file: {cleanup_err}")
