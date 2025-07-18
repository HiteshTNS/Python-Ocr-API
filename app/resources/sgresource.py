import requests
import base64
import tempfile
def fetch_pdf_base64(file_id):
    url = "https://qtsqafrws.sginternal.com/imaging-wrapper-service/getDocument"
    payload = {
        "clientId": "3",
        "processId": "4",
        "channelId": "xyy",
        "processName": "yy",
        "applicationId": "y-9A73-yy-y-y",
        "fileId": file_id,
        "isEncrypted": "False",
        "userName": ".com",
        "password": "@123"
    }
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    # Basic checks:
    if data.get("status") != "SUCCESS" or data.get("statusCode") != "200":
        raise ValueError("Failed to fetch base64 PDF: {}".format(data.get("message")))
    pdf_base64 = data["data"]["data"]
    return pdf_base64

def save_base64_to_pdf(pdf_base64):
    pdf_bytes = base64.b64decode(pdf_base64)
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_pdf:
        tmp_pdf.write(pdf_bytes)
        return tmp_pdf.name  # Returns the temp file path