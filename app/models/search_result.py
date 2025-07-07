from pydantic import BaseModel
from typing import List

class SearchResult(BaseModel):
    files: List[str]
