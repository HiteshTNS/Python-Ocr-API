import logging
from fastapi import Request, FastAPI
from starlette.responses import JSONResponse
from app.Exception.NoMatchFoundException import NoMatchFoundException
from app.resources import claim
from app.middleware.correlation_middleware import CorrelationIdMiddleware

#  Custom Formatter that handles missing correlation_id
class SafeFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, "correlation_id"):
            record.correlation_id = "N/A"
        return super().format(record)
# FastAPI app
app = FastAPI(
    title="Claims Document Extraction API",
    description="Extracts and searches claim documents from PDFs.",
    version="1.0.0"
)
logging.basicConfig(
    level=logging.INFO,  # or DEBUG
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
app.add_middleware(CorrelationIdMiddleware)
app.include_router(claim.router)
@app.exception_handler(NoMatchFoundException)
async def no_match_found_exception_handler(request: Request, exc: NoMatchFoundException):
    return JSONResponse(
        status_code=404,
        content={
            "detail": f"No value matching with the keyword: '{exc.keyword}'",
            "correlation_id": getattr(request.state, "correlation_id", "N/A")
        }
    )

