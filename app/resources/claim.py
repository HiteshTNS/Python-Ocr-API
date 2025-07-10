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

router = APIRouter()

def has_json_files(folder: str) -> bool:
    return os.path.exists(folder) and any(
        f.lower().endswith('.json') for f in os.listdir(folder)
    )

@router.post("/searchPdfDocuments")
def search_pdf_documents(
    search_params: SearchRequest = Body(default={}),
    extractDocuments: bool = Query(False, description="Set to true to trigger extraction")
):
    folder_path = r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF"
    output_json = r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF\Extracted_Json_Files"
    output_destination_folder = r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF\destination"
    batch_size = 5
    total_input_files = len([f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')])
    # total_output_files = len([f for f in os.listdir(output_destination_folder) if f.lower().endswith('.pdf')])

    extraction_needed = extractDocuments or not has_json_files(output_json)
    # extraction_status = "Applied" if extraction_needed else "Not Applied"

    try:
        search_dict = {k: v for k, v in search_params.dict().items() if v}
        # If extraction is needed, perform extraction first
        if extraction_needed:
            success, json_file, message, extracted_count, total_files = process_all_pdfs(
                folder_path, output_json, batch_size
            )
            # If search params provided, perform search after extraction
            if search_dict:
                matching_files = search_claim_documents(search_dict, folder_path, output_json)
                total_output_files = len([f for f in os.listdir(output_destination_folder) if f.lower().endswith('.pdf')])
                return {
                    "ExtractionStatus": "Applied",
                    "Extraction_Completed":f"{success}",
                    "Message": "Extraction completed with search",
                    "Summary": f"{total_output_files} of {total_input_files} documents moved to destination based on the search criteria",
                    "files": matching_files
                }
            # If no search params, just return extraction summary
            return {
                "Extraction_Completed": f"{success}",
                "Message": message if message else "Extraction Completed, proceed with search",
                "Summary": f"{extracted_count} of {total_files} documents extracted",
            }
        # If only search is needed (no extraction)
        if not search_dict:
            raise HTTPException(status_code=400, detail="No search parameters provided.")

        matching_files = search_claim_documents(search_dict, folder_path, output_json)
        total_output_files = len([f for f in os.listdir(output_destination_folder) if f.lower().endswith('.pdf')])
        return {
            "ExtractionStatus": "Not Applied",
            "Message": "Extraction completed with search",
            "Summary": f"{total_output_files} of {total_input_files} documents moved to destination based on the search criteria",
            "files": matching_files
        }
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
