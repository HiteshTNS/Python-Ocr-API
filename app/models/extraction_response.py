from pydantic import BaseModel

class ExtractionResponse(BaseModel):
    Extraction_Completed: bool
    # json_file: str
