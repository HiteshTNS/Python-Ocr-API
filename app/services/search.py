# search.py

import logging
from typing import List, Dict

logger = logging.getLogger(__name__)


def search_keywords_in_pdf(
        all_page_text: List[str], keywords: List[str], return_only_filtered: bool = False
) -> Dict:
    imageToTextSearchResponse = []
    any_keyword_found = False

    for idx, page_text in enumerate(all_page_text):
        # Ensure full keyword match, not per character
        matched_keywords = [kw for kw in keywords if kw.lower() in page_text.lower()]
        cleaned_text = page_text.replace('\n', ' ')
        if matched_keywords:
            any_keyword_found = True
            imageToTextSearchResponse.append({
                "pageNO": idx + 1,
                "keywordMatched": True,
                "selectedKeywords": "|".join(matched_keywords),
                "pageContent": cleaned_text
            })

    if not any_keyword_found:
        return {
            "imageToTextSearchResponse": {
                "keywordMatched": False,
                "selectedKeywords": "NOT FOUND",
                "pageContent": "null"
            }
        }

    if not return_only_filtered:
        return {
            "imageToTextSearchResponse": imageToTextSearchResponse,
            "imageToTextfullResponse": " ".join([t.replace('\n', ' ').replace('\\', ' ') for t in all_page_text])
        }
    else:
        return {
            "imageToTextSearchResponse": imageToTextSearchResponse
        }