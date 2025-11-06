"""Common functions for datastream manipulation."""

from typing import AsyncIterable

DEFAULT_STRING_BUFFER_SIZE = 500 * 1024 * 1024  # 500Kb


async def read_from_async_iterable(data_stream: AsyncIterable[bytes], bytes_to_read: int) -> tuple[bytes, bool]:
    """Read up to the requested number of bytes and return the read bytes and if the iterable is exhausted."""
    cur_chunk: list[bytes] = []
    bytes_read = 0
    async for d_chunk in data_stream:
        cur_chunk.append(d_chunk)
        bytes_read += len(d_chunk)
        if bytes_read >= bytes_to_read:
            break
    read_bytes = b"".join(cur_chunk)
    # End of file when we are getting less bytes than we requested.
    is_end_of_file = bytes_to_read > bytes_read
    return read_bytes, is_end_of_file


def basename(filepath: str) -> str:
    """Return base filename catering for mulitple platforms.

    This is best effort as other platforms' valid file name
    chars may overlap path separator of another.
    """
    if "\\" in filepath:
        return filepath.split("\\")[-1]
    if "/" in filepath:
        return filepath.split("/")[-1]
    return filepath
