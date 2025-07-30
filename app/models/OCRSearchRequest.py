from typing import Dict, List, Optional

from pydantic import BaseModel


class OCRSearchRequest(BaseModel):
    file_Id: str
    keywords: str  # e.g. {"CLAIMS": "", "CONTRACT": "", ...}
    returnOnlyFilteredPages: bool = False
    base64_pdf: Optional[str] = None    # New field for direct PDF content
