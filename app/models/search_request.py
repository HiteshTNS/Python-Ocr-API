from typing import Optional

from pydantic import BaseModel

class SearchRequest(BaseModel):
    Dealer: Optional[str] = None
    VIN: Optional[str] = None
    Contract: Optional[str] = None
    Claim: Optional[str] = None
    Invoice_Date: Optional[str] = None
    searchbyany: Optional[str] = None

