import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

def search_keywords_in_pdf(
    all_page_text: list, keywords: list, return_only_filtered: bool = False
) -> dict:
    imageToTextSearchResponse = []
    any_keyword_found = False

    for idx, page_text in enumerate(all_page_text):
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
        # elif not return_only_filtered:
        #     # Only include non-matching pages if returnOnlyFilteredPages is False
        #     imageToTextSearchResponse.append({
        #         "pageNO": idx + 1,
        #         "keywordMatched": False,
        #         "selectedKeywords": "",
        #         "pageContent": cleaned_text
        #     })

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
            "imageToTextfullResponse": " ".join([t.replace('\n', ' ') for t in all_page_text])
        }
    else:
        return {
            "imageToTextSearchResponse": imageToTextSearchResponse
        }
