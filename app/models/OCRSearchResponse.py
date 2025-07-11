from typing import List

from pydantic import BaseModel

from app.models.OCRSearchResult import OCRSearchResult


class OCRSearchResponse(BaseModel):
    searchResponse: List[OCRSearchResult]
    allPageText: List[str]
    imageToTextFullResponse: str