from pydantic import BaseModel

class ExtractionResponse(BaseModel):
    message: str
    json_file: str
