from typing import Dict, List

from pydantic import BaseModel


class OCRSearchRequest(BaseModel):
    file_Id: str
    keywords: List[str]  # e.g. {"CLAIMS": "", "CONTRACT": "", ...}