"""Datastram manipulation involving strings."""

import logging
import re
from typing import AsyncIterable

from azul_bedrock.models_restapi import binaries_data as bedr_bdata

from azul_metastore.common import data_common

logger = logging.getLogger(__name__)


async def get_strings(
    data_stream: AsyncIterable[bytes],
    min_length: int,
    max_length: int,
    offset: int,
    find_string: str | None = None,
    find_regex: re.Pattern[str] | None = None,
    strings_to_read_before_stopping: int = 10000,  # NOTE - may read more than this but will stop at this chunk
    buffer_size: int = data_common.DEFAULT_STRING_BUFFER_SIZE,
) -> tuple[list[bedr_bdata.SearchResult], int, bool]:
    """Return ascii and utf16 strings from data blobs, filtering them with the provided filters.

    return matching strings, total amount of content read, and True if there are more strings available.
    """
    if strings_to_read_before_stopping <= 0:
        return []
    # regex taken from https://github.com/mandiant/flare-floss/blob/master/floss/strings.py (Apache License 2.0)
    ASCII_BYTE = rb" !\"#\$%&\'\(\)\*\+,-\./0123456789:;<=>\?@ABCDEFGHIJKLMNOPQRSTUVWXYZ\[\]\^_`abcdefghijklmnopqrstuvwxyz\{\|\}\\\~\t"  # noqa: E501
    ASCII_RE = re.compile(rb"([%s]{%d,})" % (ASCII_BYTE, min_length))
    UTF16_RE = re.compile(rb"((?:[%s]\x00){%d,})" % (ASCII_BYTE, min_length))

    if find_string:
        find_string = find_string.lower()
    seen_offsets: set[int] = set()

    def function_filter_string(in_str: str, offset: int) -> bool:
        # Filter out string if they don't contain the search string
        if find_string is not None and find_string not in in_str.lower():
            return True
        # Filter out string if it doesn't match regex
        elif find_regex is not None and find_regex.search(in_str) is None:
            return True

        # Filter out the string if it's been seen before, this occurs because the file is processed in chunks
        # And the chunks need to be overlapped to catch strings between the chunks.
        if offset in seen_offsets:
            return True
        seen_offsets.add(offset)

        return False

    read_content_length = 0
    reached_end_of_file = False
    cur_chunk: bytes = b""
    strings = []
    last_chunk_offset = offset
    """Read through the file in chunks joining the chunks as required.
    This is done with an A, B chunk system and filtering out duplicate strings.
    This means no strings are missed if they are on the boundary between chunks as per this example (8 byte chunk size)
    e.g b"abcdeffindmeghij"
    Searching for the string 'findme' and 'ab'
    iteration 1
    chunk_A = b"abcdeffi" # Chunk A has no string 'findme' but has 'ab'
    iteration 2
    chunk_A + chunk_B = b"ndmeghij" # Chunk A + B has a string 'findme' and 'ab' but we need to filter out ab because
    # it was found in iteration 1.
    """
    max_chunk_size = max(max_length, buffer_size)

    while reached_end_of_file is False:
        # Take as little as the last chunk as possible and still not miss any strings (x2 to avoid splitting a result
        # in half) if on the boundary between ascii characters.
        last_chunk_fragment: bytes = cur_chunk[-max_length * 2 :]
        last_chunk_offset += len(cur_chunk) - len(last_chunk_fragment)
        cur_chunk, reached_end_of_file = await data_common.read_from_async_iterable(data_stream, max_chunk_size)
        read_content_length += len(cur_chunk)

        current_window = last_chunk_fragment + cur_chunk

        for match in ASCII_RE.finditer(current_window):
            try:
                string = match.group().decode("ascii")
                # Skip for now, as this match may be longer.
                # Because we are up to the boundary of the chunk and we haven't hit the end of the file yet
                if match.end() == read_content_length and not reached_end_of_file:
                    continue

                string_offset = last_chunk_offset + match.start()
                if function_filter_string(string, string_offset):
                    continue
            except UnicodeDecodeError:
                pass
            else:
                # Drop excessively long strings
                if len(string) > max_length:
                    continue
                strings.append(
                    bedr_bdata.SearchResult(
                        string=string,
                        offset=string_offset,
                        length=match.end() - match.start(),
                        encoding=bedr_bdata.SearchResultType.ASCII,
                    )
                )

        for match in UTF16_RE.finditer(current_window):
            try:
                string = match.group().decode("utf-16")
                # Skip for now, as this match may be longer.
                # Because we are up to the boundary of the chunk and we haven't hit the end of the file yet
                if match.end() == read_content_length and not reached_end_of_file:
                    continue

                string_offset = last_chunk_offset + match.start()
                if function_filter_string(string, string_offset):
                    continue
            except UnicodeDecodeError:
                pass
            else:
                # Drop excessively long strings
                if len(string) > max_length:
                    continue
                strings.append(
                    bedr_bdata.SearchResult(
                        string=string,
                        offset=string_offset,
                        length=match.end() - match.start(),
                        encoding=bedr_bdata.SearchResultType.UTF16,
                    )
                )

        # We've found the max number of strings return
        if len(strings) >= strings_to_read_before_stopping:
            break
    return strings, read_content_length, not reached_end_of_file
