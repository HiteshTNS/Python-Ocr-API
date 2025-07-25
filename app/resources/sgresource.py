import requests
import base64
import tempfile
def fetch_pdf_base64(file_id):
    url = "https://qtsqafrws.sginternal.com/imaging-wrapper-service/getDocument"
    payload = {
        "clientId": "3",
        "processId": "4",
        "channelId": "SGINTLCMS",
        "processName": "CANCEL",
        "applicationId": "8C96F819-9A73-4C8D-8A2B-ED84989CC38C",
        "fileId": file_id,
        "isEncrypted": "False",
        "userName": "admin@sgintl.com",
        "password": "Test@123"
    }
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    # Basic checks:
    if data.get("status") != "SUCCESS" or data.get("statusCode") != "200":
        raise ValueError("Failed to fetch base64 PDF: {}".format(data.get("message")))
    pdf_base64 = data["data"]["data"]
    pdf_bytes = base64.b64decode(pdf_base64)
    return pdf_bytes


def test_pdf_code(file_id=None):  # file_id is optional since /getDocuments is hardcoded
    url = "http://127.0.0.1:8000/getDocuments"

    try:
        response = requests.post(url)
        response.raise_for_status()  # handle HTTP errors

        data = response.json()
        if data.get("status") != "success":
            raise ValueError("API returned failure status.")

        base64_pdf = data.get("base64PDF")
        if not base64_pdf:
            raise ValueError("No base64PDF found in response.")

        pdf_bytes = base64.b64decode(base64_pdf)
        print("PDF loaded into memory (bytes), size:", len(pdf_bytes))

        return pdf_bytes

    except Exception as e:
        print("Error during PDF base64 test:", e)
        return None