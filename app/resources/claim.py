import os
import tempfile
import zipfile
from fastapi import APIRouter, Query, HTTPException, Body
from typing import List
from starlette.responses import FileResponse
from app.models.search_request import SearchRequest
from app.Exception.NoMatchFoundException import NoMatchFoundException
from app.services.process_all_pdfs import process_all_pdfs
from app.services.search import search_claim_documents
from app.models.config import AppSettings
from app.utils.s3_utils import (
    get_s3_client,
    list_pdfs_in_s3,
    list_jsons_in_s3,
    list_files_in_s3_prefix,
    download_s3_file,
)

router = APIRouter()

env_profile = os.environ.get("APP_PROFILE", "uat")  # default to uat
env_file = f".env.{env_profile}"
settings = AppSettings(_env_file=env_file)

def has_json_files_in_s3():
    s3 = get_s3_client()
    json_keys = list_jsons_in_s3(
        s3,
        bucket=settings.s3_source_bucket,
        prefix=settings.pdf_json_output_prefix
    )
    return len(json_keys) > 0

@router.post("/searchPdfDocuments")
def search_pdf_documents(
    search_params: SearchRequest = Body(default={}),
    extractDocuments: bool = Query(False, description="Set to true to trigger extraction")
):
    batch_size = settings.batch_size

    extraction_needed = extractDocuments or not has_json_files_in_s3()
    # extraction_status = "Applied" if extraction_needed else "Not Applied"
    total_input_files = len(list_pdfs_in_s3())  # total input PDFs
    try:
        search_dict = {k: v for k, v in search_params.dict().items() if v}
        # Extraction phase
        if extraction_needed:
            success, message, extracted_count, total_files = process_all_pdfs(batch_size)
            if search_dict:
                # After extraction, perform search if search params provided
                matching_files = search_claim_documents(search_dict, settings)
                # Count source and destination files in S3
                s3 = get_s3_client()

                total_files_in_destination = len(list_files_in_s3_prefix(
                    s3,
                    bucket=settings.s3_destination_bucket,
                    prefix=""
                ))  # total moved to destination
                return {
                    "ExtractionStatus": "Applied",
                    "Message": "Extraction completed with search",
                    "Summary": f"{total_files_in_destination} of {total_input_files} documents moved to destination based on the search criteria",
                    "files": matching_files
                }
            # If no search params, just return extraction summary
            return {
                "Extraction_Completed": f"{success}",
                "Message": message if message else "Extraction Completed, proceed with search",
                "Summary": f"{extracted_count} of {total_files} documents extracted",
            }
        # Only search phase
        if not search_dict:
            raise HTTPException(status_code=400, detail="No search parameters provided.")

        matching_files = search_claim_documents(search_dict, settings)
        s3 = get_s3_client()
        # x = len(list_pdfs_in_s3())
        total_files_in_destination = len(list_files_in_s3_prefix(
            s3,
            bucket=settings.s3_destination_bucket,
            prefix=""
        ))
        return {
            "ExtractionStatus": "Not Applied",
            "Message": "Extraction completed with search",
            "Summary": f"{total_files_in_destination} of {total_input_files} documents moved to destination based on the search criteria",
            "files": matching_files
        }
    except NoMatchFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/download/all")
def download_all_files():
    """
    Downloads all files from the destination S3 bucket as a zip and returns it as a response.
    """
    s3 = get_s3_client()
    try:
        # List all files in the destination bucket
        files = list_files_in_s3_prefix(
            s3,
            bucket=settings.s3_destination_bucket,
            prefix=""
        )
        if not files:
            raise HTTPException(status_code=404, detail="No files to download.")

        # Download all files to a temp dir and zip them
        with tempfile.TemporaryDirectory() as tmpdir:
            for key in files:
                local_path = os.path.join(tmpdir, os.path.basename(key))
                s3.download_file(settings.s3_destination_bucket, key, local_path)
            # Create a zip in a temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmpzip:
                with zipfile.ZipFile(tmpzip.name, 'w') as zipf:
                    for fname in os.listdir(tmpdir):
                        zipf.write(os.path.join(tmpdir, fname), arcname=fname)
                tmpzip_path = tmpzip.name
        return FileResponse(path=tmpzip_path, filename="destination_files.zip", media_type='application/zip')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/healthcheck")
def healthcheck():
    return {"status": "ok"}
