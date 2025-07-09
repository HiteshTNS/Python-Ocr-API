from typing import Optional

from pydantic import BaseModel

class ExtractionResponse(BaseModel):
    Extraction_Completed: bool
    message: Optional[str] = None
    Summary: str
