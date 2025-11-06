"""Datastram manipulation involving hex."""

import logging
from typing import AsyncIterable, ByteString

from azul_bedrock.models_restapi import binaries_data as bedr_bdata

from azul_metastore.common import data_common

logger = logging.getLogger(__name__)


async def get_hex_hits(
    data_stream: AsyncIterable[bytes],
    offset: int,
    max_results: int,
    pattern: ByteString,
    buffer_size: int = data_common.DEFAULT_STRING_BUFFER_SIZE,
) -> tuple[list[bedr_bdata.SearchResult], int, bool]:
    """Returns a list of hits for a particular pattern in the given data blob.

    also returns total bytes of the file read and a boolean that is true if there is more file content to read.
    """
    hits = []

    if max_results <= 0:
        return hits

    seen_offsets: set[int] = set()

    # Prepare a string to render the user pattern as if we find hits
    string = ascii_group_formatter(pattern, strip=False)

    end_of_file = False
    last_chunk_fragment = b""
    length_of_file = 0
    last_chunk_offset = offset
    # Times by 2 to avoid finding a half result
    last_chunk_length = len(pattern) * 2

    while end_of_file is False:
        cur_chunk, end_of_file = await data_common.read_from_async_iterable(data_stream, buffer_size)
        length_of_file += len(cur_chunk)
        window = last_chunk_fragment + cur_chunk

        starting_index = 0
        while True:
            hit_index = window.find(pattern, starting_index)
            starting_index = hit_index + 1
            hit_offset = last_chunk_offset + hit_index
            if hit_index == -1:
                break
            # Filter out duplicate hits.
            if hit_offset in seen_offsets:
                continue

            seen_offsets.add(hit_offset)
            hits.append(
                bedr_bdata.SearchResult(
                    string=string,
                    offset=last_chunk_offset + hit_index,
                    length=len(pattern),
                    encoding=bedr_bdata.SearchResultType.Hex,
                )
            )
            # If max hits reached exit
            if len(hits) >= max_results:
                break

        # If max hits reached exit
        if len(hits) >= max_results:
            break

        # Take as little of the last chunk as possible to avoid missing something between the chunks
        # Multiple last
        last_chunk_fragment = cur_chunk[-last_chunk_length * 2 :]
        last_chunk_offset += len(cur_chunk) - len(last_chunk_fragment)

    return hits, length_of_file, not end_of_file


# Ascii range of printable chars
_PRINTABLES_BEGIN = 33
_PRINTABLES_END = 126


def ascii_group_formatter(iterable, strip=True) -> str:
    """Strip or escape non-printable chars from chuck to allow printability."""
    characters = []

    for x in iterable:
        if x == ord("\\") and not strip:
            # Double escape to make it obvious to the reader what
            # is our escape and what is external escaping
            characters.append("\\\\")
        elif _PRINTABLES_BEGIN <= x <= _PRINTABLES_END:
            characters.append(chr(x))
        elif strip:
            characters.append(".")
        else:
            characters.append("\\x%02x" % x)

    return "".join(characters)
