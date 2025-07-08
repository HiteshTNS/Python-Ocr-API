from typing import Optional
from pydantic import BaseModel, Field

class SearchRequest(BaseModel):
    Dealer: Optional[str] = Field(None, alias="Dealer Name")
    VIN: Optional[str] = None
    Contract: Optional[str] = Field(None, alias="Contract #")
    Claim: Optional[str] = Field(None, alias="Claim #")
    searchbyany: Optional[str] = Field(None, alias="Search by Word")
