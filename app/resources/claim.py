import base64
import os
import logging
import asyncio
import time
from functools import partial
from fastapi import APIRouter, HTTPException, status, BackgroundTasks
from fastapi.responses import JSONResponse
import httpx
# from app.models.config import AppSettings
from app.models.OCRSearchRequest import OCRSearchRequest
from app.services.search import search_keywords_live_parallel  # This is blocking CPU code, will run async-wrapped
from app.resources.sgresource import fetch_pdf_base64
from app.utils.http_utils import post_ocr_result_to_db_async

router = APIRouter()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ocr_semaphore = asyncio.Semaphore(10)  # Allow up to 10 OCR jobs at once

async def async_search_keywords_live_parallel(*args, **kwargs):
    """
    Wraps the blocking OCR search function in a thread pool for async compatibility.
    """
    loop = asyncio.get_running_loop()
    # Run the blocking function in a separate thread
    return await loop.run_in_executor(None, lambda: search_keywords_live_parallel(*args, **kwargs))


@router.post("/getDocumentwithOCRSearchPyMuPdf", status_code=200)
async def get_document_with_ocr_search(request: OCRSearchRequest, background_tasks: BackgroundTasks):
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

    # Fetch base64 PDF from internal API (blocking sync, so run in thread)
    loop = asyncio.get_running_loop()
    try:
        pdf_base64 = await loop.run_in_executor(None, lambda: fetch_pdf_base64(file_id))
        # pdf_base64 = await loop.run_in_executor(None, lambda: test_pdf_code(file_id))

    except Exception as e:
        logger.error(f"Failed to fetch or decode PDF for file_id {file_id}: {e}")
        raise HTTPException(status_code=404, detail=f"Unable to fetch PDF for file_id {file_id}")

        # OCR with concurrency limit
    async with ocr_semaphore:
        start_time = time.time()

        # Prepare the callable with pre-filled args
        ocr_task = partial(
            search_keywords_live_parallel,
            pdf_bytes=pdf_base64,
            keywords=keywords,
            return_only_filtered=return_only_filtered
        )

        # Then run it in executor
        search_response = await loop.run_in_executor(None, ocr_task)

        end_time = time.time()
        logger.info(f"PDF processing and search took {end_time - start_time:.2f} seconds")

    # Offload POST callback to background task
    background_tasks.add_task(post_ocr_result_to_db_async, file_id, keywords, search_response, max_retries=3, retry_delay=2.0, error_callback=log_post_error)

    # Immediately return the OCR result so the client doesn't wait for downstream post
    return JSONResponse(status_code=status.HTTP_200_OK, content=search_response)


async def log_post_error(exc: Exception, **kwargs):
    file_id = kwargs.get("file_id")
    logger.error(f"Final failure posting OCR result for file_id {file_id}: {exc}")

@router.post("/getDocuments")
def get_base64_pdf():
    """
    Returns a base64-encoded string of a hardcoded PDF file
    """
    # Replace this with your actual file path as needed
    pdf_file_path = r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF\PIUNTI 108721.pdf"  # e.g., "files/sample.pdf"

    if not os.path.exists(pdf_file_path):
        raise HTTPException(status_code=404, detail="PDF file not found.")

    try:
        with open(pdf_file_path, "rb") as pdf_file:
            pdf_bytes = pdf_file.read()
            base64_str = base64.b64encode(pdf_bytes).decode("utf-8")

        return JSONResponse(
            content={"status": "success", "base64PDF": base64_str}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to encode PDF: {str(e)}")

@router.post("/receive-ocr-result")
def receive_ocr_result(payload: dict):
    print("Received OCR result:")
    # print(payload)
    return {"status": "received", "message": "Data stored successfully"}