from pydantic import BaseModel


class OCRSearchResult(BaseModel):
    Keyword: str
    PageNo: int
    PageText: str