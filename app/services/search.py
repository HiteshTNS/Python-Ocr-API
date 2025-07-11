import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

def search_keywords_in_pdf(all_page_text: List[str], keywords: List[str]) -> Dict:
    keyword_results = {}
    matching_pages_set = set()

    # 1. Find filtered pages for each keyword
    for keyword in keywords:
        filtered_pages = []
        for idx, page_text in enumerate(all_page_text):
            if keyword.lower() in page_text.lower():
                filtered_pages.append(idx + 1)  # Page numbers are 1-based
                matching_pages_set.add(idx)
        keyword_results[keyword] = {"filtered_pages": filtered_pages}

    # 2. Prepare page_data for only matching pages
    page_data = [
        {"page_no": idx + 1, "page_text": all_page_text[idx]}
        for idx in sorted(matching_pages_set)
    ]

    # 3. Concatenate all page texts for imageToTextFullResponse
    imageToTextFullResponse = "\n".join(all_page_text)

    return {
        "keywords": keyword_results,
        "page_data": page_data,
        "imageToTextFullResponse": imageToTextFullResponse
    }
