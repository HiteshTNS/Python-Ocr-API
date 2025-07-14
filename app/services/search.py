import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

def search_keywords_in_pdf(
    all_page_text: List[str], keywords: List[str], return_only_filtered: bool = False
) -> Dict:
    imageToTextSearchResponse = []
    any_keyword_found = False

    for idx, page_text in enumerate(all_page_text):
        matched_keywords = [kw for kw in keywords if kw.lower() in page_text.lower()]
        keywordMatched = bool(matched_keywords)
        any_keyword_found = any_keyword_found or keywordMatched

        # If returnOnlyFilteredPages is True, only include matched pages
        if return_only_filtered and not keywordMatched:
            continue

        imageToTextSearchResponse.append({
            "pageNO": idx + 1,
            "keywordMatched": keywordMatched,
            "selectedKeywords": "|".join(matched_keywords) if keywordMatched else ("NOT FOUND" if not keywordMatched else ""),
            "pageContent": page_text if (not return_only_filtered or keywordMatched) else None
        })

    # If no keywords found on any page, return special response
    if not any_keyword_found:
        return {
            "imageToTextSearchResponse": {
                "keywordMatched": False,
                "selectedKeywords": "NOT FOUND",
                "pageContent": "null"
            }
        }

    # If returnOnlyFilteredPages is False, add full response
    if not return_only_filtered:
        return {
            "imageToTextSearchResponse": imageToTextSearchResponse,
            "imageToTextfullResponse": "\n".join(all_page_text)
        }
    else:
        return {
            "imageToTextSearchResponse": imageToTextSearchResponse
        }
