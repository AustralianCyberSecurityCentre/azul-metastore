"""API call to the AI string filter service."""

import logging

import httpx
from azul_bedrock import models_restapi
from azul_bedrock.exceptions import ApiException

logger = logging.getLogger(__name__)


def call_string_filter(
    file_format: str, strings: list[models_restapi.SearchResult], filter_url: str
) -> list[dict[str, int]]:
    """Call the AI string filter."""
    base_url = f"{filter_url}/v0/strings?file_format={file_format}"
    modified_strings = extract_string_and_offset(strings)
    logger.debug("calling ai string filter with URL %s", base_url)
    response = httpx.post(base_url, json=modified_strings, timeout=30)

    if response.is_error:
        error_message = f"AI filter failed with error: {response.content}"
        logger.warning(error_message)
        raise ApiException(
            status_code=response.status_code,
            ref=error_message,
            internal="string_filter_error",
        )

    if response is not None:
        return response.json()
    return []


def extract_string_and_offset(searchResult: list[models_restapi.SearchResult]) -> list[dict[str, int]]:
    """Extract the string and offset from each SearchResult.strings."""
    filtered_results = [{"string": sr.string, "offset": sr.offset} for sr in searchResult]
    return filtered_results


def filter_search_results(
    full_search_object: list[models_restapi.SearchResult], partial_search_object: list[dict[str, int]]
) -> list[models_restapi.SearchResult]:
    """Filters out results not returned by ai string filter in SearchResult.strings."""
    valid_pairs = {(item["offset"], item["string"]) for item in partial_search_object}
    filtered_results = [sr for sr in full_search_object if (sr.offset, sr.string) in valid_pairs]

    return filtered_results
