import logging
import re
from typing import List, Dict, Union

logger = logging.getLogger(__name__)

def search_keywords_in_pdf(
    all_page_text: List[str], keywords: List[str], return_only_filtered: bool = False
) -> Dict[str, Union[List[Dict], Dict]]:
    imageToTextSearchResponse = []
    any_keyword_found = False

    # Compile regex patterns for each keyword to match exact word (word boundaries)
    # Example: r'\bcontract\b' to match only the exact word
    keyword_patterns = {
        kw: re.compile(rf'\b{re.escape(kw)}\b', re.IGNORECASE) for kw in keywords
    }

    for idx, page_text in enumerate(all_page_text):
        matched_keywords = []

        # Check each pattern using regex word-boundary matching
        for kw, pattern in keyword_patterns.items():
            if pattern.search(page_text):
                matched_keywords.append(kw)

        cleaned_text = page_text.replace('\n', ' ')

        if matched_keywords:
            any_keyword_found = True
            imageToTextSearchResponse.append({
                "pageNO": idx + 1,
                "keywordMatched": True,
                "selectedKeywords": "|".join(matched_keywords),
                "pageContent": cleaned_text
            })
        elif not return_only_filtered:
            # Include non-matching pages only if return_only_filtered is False
            imageToTextSearchResponse.append({
                "pageNO": idx + 1,
                "keywordMatched": False,
                "selectedKeywords": "",
                "pageContent": cleaned_text
            })

    # If no keywords found at all, return special fallback response
    if not any_keyword_found:
        return {
            "imageToTextSearchResponse": {
                "keywordMatched": False,
                "selectedKeywords": "NOT FOUND",
                "pageContent": "null"
            }
        }

    return {
        "imageToTextSearchResponse": imageToTextSearchResponse
    }
