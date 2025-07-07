from typing import Optional

from fastapi import APIRouter, Query, HTTPException, Body

from app.Exception.NoMatchFoundException import NoMatchFoundException
from app.models.extraction_response import ExtractionResponse
from app.models.search_request import SearchRequest
from app.models.search_result import SearchResult
from app.services.process_all_pdfs import process_all_pdfs
from app.services.search import search_claim_documents

router = APIRouter()

@router.post("/extractclaimdocuments", response_model=ExtractionResponse)
def extract_claim_documents():
    try:
        folder_path = r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF"  # Or get from config/env
        output_json = r"C:\Users\hitesh.paliwal\Desktop\claims_data.json"
        json_file = process_all_pdfs(folder_path, output_json)
        return ExtractionResponse(
            message="All files are processed and stored.",
            json_file=json_file
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
    try:
        # Convert Pydantic model to dictionary and pass to your search function
        matching_files = search_claim_documents(search_params.dict())
        return SearchResult(files=matching_files)
    except NoMatchFoundException as e:
        raise e  # Will be handled by your global exception handler

@router.get("/healthcheck")
def healthcheck():
    return {"status": "ok"}