import asyncio
import json
import time
import httpx
from openpyxl import Workbook

API_URL = "http://127.0.0.1:8000/getDocumentwithOCRSearchPyMuPdf"  # Adjust if needed

# Global storage
timings = []
results = []

async def run_ocr_test():
    path = r"c:\Users\st\Desktop\sample.txt"
    with open(path, "r") as f:
        base64_pdf = f.read().strip()

    # For testing, reduced to 5 calls; change to 1000 when ready
    test_data = [
        {"fileId": f"fileid_{i+1}", "base64_pdf": base64_pdf} for i in range(1000)
    ]

    semaphore = asyncio.Semaphore(1)  # Limit concurrency, adjust as needed

    async def post_ocr(item):
        async with semaphore:
            payload = {
                "file_Id": item["fileId"],
                "keywords": "CLAIM|INVOICE|CONTRACT",
                "base64_pdf": item["base64_pdf"],
            }
            async with httpx.AsyncClient(timeout=None) as client:
                try:
                    start_time = time.time()
                    response = await client.post(API_URL, json=payload)
                    elapsed = time.time() - start_time
                    timings.append(elapsed)

                    # Try to parse JSON response, otherwise fallback to raw text
                    try:
                        resp_json = response.json()
                        resp_data = resp_json
                    except Exception:
                        resp_data = response.text

                    # Append full info to results
                    results.append({
                        "fileId": item["fileId"],
                        "status_code": response.status_code,
                        "elapsed_seconds": elapsed,
                        "response": resp_data
                    })

                    print(f"{item['fileId']} status: {response.status_code} time: {elapsed:.2f}s")

                except Exception as e:
                    print(f"Request failed for {item['fileId']}: {e}")
                    timings.append(None)
                    results.append({
                        "fileId": item["fileId"],
                        "status_code": None,
                        "elapsed_seconds": None,
                        "response": f"Exception: {str(e)}"
                    })

    total_start_time = time.time()
    await asyncio.gather(*(post_ocr(item) for item in test_data))
    total_elapsed_time = time.time() - total_start_time

    # Write results to JSON file (full responses)
    json_output_file = "ocr_results.json"
    with open(json_output_file, "w", encoding="utf-8") as f_json:
        json.dump(results, f_json, indent=2, ensure_ascii=False)

    print(f"Saved full OCR responses for {len(results)} calls to '{json_output_file}'.")

    # Write timings separately to an Excel file with total time in 3rd column
    wb = Workbook()
    ws = wb.active
    ws.title = "OCR Timings"
    ws.append(["Call Number", "Processing Time (seconds)", "Total Time (seconds)"])

    for i, t in enumerate(timings, start=1):
        ws.append([i, t if t is not None else "Failed", None])
    # Add total elapsed time in the last row, third column
    ws.append([None, None, total_elapsed_time])

    excel_output_file = "ocr_processing_timings.xlsx"
    wb.save(excel_output_file)
    print(f"Saved timings for {len(timings)} calls to '{excel_output_file}'.")
    print(f"Total elapsed time for all calls: {total_elapsed_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(run_ocr_test())
