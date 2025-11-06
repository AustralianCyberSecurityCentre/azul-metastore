import os
import re

import httpx
from azul_bedrock.models_restapi.basic import UserAccess, UserSecurity


def get_file(target: str):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../testdata", target)
    return open(path, "rb")


def mock_get_user_access(self):
    """Fake method for getting users security context."""
    return UserAccess(security=UserSecurity(labels=["LOW", "MOD1"]))


def mock_get_user_access_medium(*args):
    """Fake method for getting users security context at a medium level."""
    return UserAccess(security=UserSecurity(labels=["LOW", "TLP:CLEAR", "MEDIUM", "MOD1", "REL:APPLE", "REL:BEE"]))


async def convert_content_to_async_iterable(content: bytes):
    """Read in fixed chunk size to make testing more realistic."""
    CHUNK_SIZE = 500  # Note changing this chunk size will have an impact on the test results.
    location = 0
    while location + CHUNK_SIZE < len(content):
        yield content[location : location + CHUNK_SIZE]
        location += CHUNK_SIZE
    # yield remainder
    if location < len(content):
        yield content[location:]


def mock_load_binary_async_iterable_content(request: httpx.Request):
    return mock_load_binary(request, True)


def mock_load_binary(request: httpx.Request, is_async_iterable_content: bool = False) -> httpx.Response:
    headers = request.headers
    response_headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": "application/octet-stream",
        # 'Date': 'Fri, 26 Jun 2020 02:09:24 GMT',
        # 'Transfer-Encoding': 'chunked',
    }
    file = request.url.path.split("/")[-1]

    start = None
    end = None

    # Extract range params
    if "Range" in headers:
        header_range = headers["Range"]

        match = re.fullmatch(r"^(bytes=)([0-9]{1,})(-)([0-9]{0,})$", header_range)
        if match is None:
            return httpx.Response(status_code=500, headers=response_headers, content=None)
        else:
            start = int(match.groups()[1])
            if match.groups()[3] != "":
                end = int(match.groups()[3])
            else:
                end = -1

    # Open file and return contents
    with get_file(f"{file}.txt") as f:
        contents = f.read()
        # Trim response if range params given
        if start is not None and end is not None:
            if end == -1:
                end = len(contents)
            response_headers["Content-Range"] = f"{start}-{end}/{len(contents)}"
            # response_headers['Transfer-Encoding']='chunked'

            if start > len(contents):
                raise Exception("Error")

            contents = contents[start : end + 1]
            # return MockResponse(contents, HTTP_206_PARTIAL_CONTENT, headers=header)
            if is_async_iterable_content:
                return httpx.Response(
                    status_code=206, headers=response_headers, content=convert_content_to_async_iterable(contents)
                )
            else:
                return httpx.Response(status_code=206, headers=response_headers, content=contents)

        if is_async_iterable_content:
            return httpx.Response(
                status_code=200, headers=response_headers, content=convert_content_to_async_iterable(contents)
            )
        else:
            return httpx.Response(status_code=200, headers=response_headers, content=contents)
