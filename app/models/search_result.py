from pydantic import BaseModel
from typing import List

class SearchResult(BaseModel):
    ExtractionStatus: str
    Message: str
    Summary: str
    files: List[str]
