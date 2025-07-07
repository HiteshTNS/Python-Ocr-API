from fastapi import Request
from fastapi import FastAPI
from starlette.responses import JSONResponse

from app.Exception.NoMatchFoundException import NoMatchFoundException
from app.resources import claim

app = FastAPI(
    title="Claims Document Extraction API",
    description="Extracts and searches claim documents from PDFs.",
    version="1.0.0"
)

app.include_router(claim.router)

@app.exception_handler(NoMatchFoundException)
async def no_match_found_exception_handler(request: Request, exc: NoMatchFoundException):
    return JSONResponse(
        status_code=404,
        content={"detail": f"No value matching with the keyword: '{exc.keyword}'"}
    )