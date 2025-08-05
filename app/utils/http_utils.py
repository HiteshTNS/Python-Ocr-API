import asyncio
import logging
from typing import List, Dict, Any, Callable, Optional
import httpx
from app.utils.email_utils import send_mail_notification
from app.utils.html_utils import build_html_body

logger = logging.getLogger(__name__)


async def post_ocr_result_to_db_async(
    file_id: str,
    keywords: List[str],
    search_result: Dict[str, Any],
    max_retries: int = 3,
    retry_delay: float = 2.0,
    error_callback: Optional[Callable] = None
) -> Dict[str, Any]:
    """
    Asynchronously posts OCR result to downstream DB API with retries and sends email notification on success.

    :param file_id: Unique file identifier
    :param keywords: List of search keywords
    :param search_result: OCR result dict containing 'imageToTextSearchResponse'
    :param max_retries: Maximum retry attempts on failure
    :param retry_delay: Delay in seconds between retries
    :param error_callback: Optional async callback on final failure
    :return: Response dict indicating success or error
    """

    url = "http://localhost:8080/documentType/update"  # Replace with actual downstream API URL

    payload = {
        "appUserId": "5",
        "clientId": "3",
        "channelId": "portal_UI",
        "name": "TINSPECT",
        "description": "Test Document Type",
        "documentTypeField": [
            {
                "attributeName": "Test #",
                "attributeDescription": "Contract Number",
                "attributeValue": search_result.get("imageToTextSearchResponse", [])
            }
        ],
        "applicationId": "8C96F819-9A73-4C8D-8A2B-ED84989CC38C",
        "fileId": file_id
    }

    for attempt in range(1, max_retries + 1):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.put(url, json=payload)
                logger.info(f"Response Status: {response.status_code}, Body: {response.text}")
                response.raise_for_status()
                resp_json = response.json()
                if resp_json.get("success") is True and resp_json.get("statusCode") == "200":
                    logger.info(f"Successfully posted OCR result to downstream API on attempt {attempt}")

                # --- EMAIL SENDING LOGIC AFTER SUCCESSFUL POST ---
                # html_body = build_html_body(payload)
                # loop = asyncio.get_running_loop()
                # await loop.run_in_executor(None, send_mail_notification, html_body, file_id)

                return response.json() or {"status": "success"}

        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logger.error(f"Attempt {attempt}: Failed to post OCR result: {e}")
            if attempt == max_retries:
                if error_callback:
                    try:
                        await error_callback(e, file_id=file_id, keywords=keywords, payload=payload)
                    except Exception as cb_exc:
                        logger.error(f"Error in error_callback: {cb_exc}")
                return {"status": "error", "message": str(e)}

        except Exception as e:
            logger.exception(f"Unexpected error on attempt {attempt}: {e}")
            if attempt == max_retries:
                return {"status": "error", "message": f"Unexpected error: {str(e)}"}

        await asyncio.sleep(retry_delay)

    return {"status": "error", "message": "Failed to post OCR result after retries"}
