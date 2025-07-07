import os
from typing import Optional

from fastapi import APIRouter, Query, HTTPException, Body

from app.Exception.NoMatchFoundException import NoMatchFoundException
from app.models.extraction_response import ExtractionResponse
from app.models.search_request import SearchRequest
from app.models.search_result import SearchResult
from app.services.process_all_pdfs import process_all_pdfs
from app.services.search import search_claim_documents
from fastapi.responses import FileResponse
import zipfile
import tempfile
router = APIRouter()

@router.post("/extractclaimdocuments", response_model=ExtractionResponse)
def extract_claim_documents():
    try:
        folder_path = r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF"
        output_json = r"C:\Users\hitesh.paliwal\Desktop\claims_data.json"
        Extraction_Completed, json_file = process_all_pdfs(folder_path, output_json)
        return ExtractionResponse(
            Extraction_Completed=Extraction_Completed
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Extraction failed: {e}")

@router.get(
    "/searchclaimsdocuments",
    response_model=SearchResult,
    summary="Search claim documents by keyword",
    description="Returns a list of PDF filenames containing the given keyword."
)
def search_claims_documents(
    search_params: SearchRequest = Body(..., description="Fields to search in claim documents")
):
    input_folder=r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF"
    json_file=r"C:\Users\hitesh.paliwal\Desktop\Python Ocr\data.json"
    try:
        # Convert Pydantic model to dictionary and pass to your search function
        matching_files = search_claim_documents(search_params.dict(),input_folder,json_file)
        return SearchResult(files=matching_files)
    except NoMatchFoundException as e:
        raise e  # Will be handled by your global exception handler

@router.get("/download/all")
def download_all_files():
    DESTINATION_FOLDER=r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF\destination"
    try:
        files = [f for f in os.listdir(DESTINATION_FOLDER) if os.path.isfile(os.path.join(DESTINATION_FOLDER, f))]
        if not files:
            raise HTTPException(status_code=404, detail="No files to download.")
        # Create a temporary zip file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
            with zipfile.ZipFile(tmp.name, 'w') as zipf:
                for f in files:
                    zipf.write(os.path.join(DESTINATION_FOLDER, f), arcname=f)
            tmp_path = tmp.name
        return FileResponse(path=tmp_path, filename="destination_files.zip", media_type='application/zip')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/healthcheck")
def healthcheck():
    return {"status": "ok"}