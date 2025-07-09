
import os
from typing import Optional, Dict, Union

from fastapi import APIRouter, Query, HTTPException, Body
from fastapi.responses import FileResponse
import zipfile
import tempfile

from app.Exception.NoMatchFoundException import NoMatchFoundException
from app.models.extraction_response import ExtractionResponse
from app.models.search_request import SearchRequest
from app.models.search_result import SearchResult
from app.services.process_all_pdfs import process_all_pdfs
from app.services.search import search_claim_documents

router = APIRouter()

@router.post(
    "/searchPdfDocuments",
    response_model=Union[ExtractionResponse, SearchResult],
    response_model_exclude_none=True,
    summary="Extract and/or search claim documents",
    description=(
        "If 'extractDocuments' is true or JSON data file is missing, "
        "performs extraction and returns extraction status. "
        "Otherwise, performs search on existing JSON data."
    )
)
def search_pdf_documents(
    search_params: SearchRequest = Body(default={}),
    extractDocuments: bool = Query(False, description="Set to true to trigger extraction")
):
    folder_path = r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF"
    output_json = r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF\Extracted_Json_Files"
    batch_size = 5

    # Check if any JSON files exist in the output folder
    def has_json_files(folder: str) -> bool:
        return os.path.exists(folder) and any(
            f.lower().endswith('.json') for f in os.listdir(folder)
        )

    # If extraction requested or no JSON files found, run extraction
    if extractDocuments or not has_json_files(output_json):
        try:
            success, json_file, message,extracted_count, total_files  = process_all_pdfs(folder_path, output_json, batch_size)
            return ExtractionResponse(
                Extraction_Completed=success,
                message=message if message else "Extraction Completed, proceed with search",
                Summary= f"{extracted_count} of {total_files} documents extracted"
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Extraction failed: {e}")

    # Otherwise, perform search
    try:
        search_dict = {k: v for k, v in search_params.dict().items() if v}
        if not search_dict:
            raise HTTPException(status_code=400, detail="No search parameters provided.")

        matching_files = search_claim_documents(search_dict, folder_path, output_json)
        return SearchResult(files=matching_files)
    except NoMatchFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/download/all")
def download_all_files():
    DESTINATION_FOLDER = r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF\destination"
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
