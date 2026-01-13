"""Download or interact with binary datastreams."""

import io
import itertools
import logging
import re
import time
from typing import AsyncIterable, Callable, Generator

import pyzipper
from azul_bedrock.exceptions import ApiException, BaseError, DispatcherApiException
from azul_bedrock.models_restapi import binaries_data as bedr_binaries_data
from cart import cart
from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Path,
    Query,
    Request,
    Response,
)
from starlette.responses import StreamingResponse
from starlette.status import HTTP_200_OK, HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND

from azul_metastore import context, settings
from azul_metastore.common import data_hex, data_strings, string_filter
from azul_metastore.common.fileformat import get_attachment_type
from azul_metastore.query.binary2 import binary_read
from azul_metastore.restapi.quick import qr

router = APIRouter()

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(name)s:%(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    level=logging.WARNING,
)


@router.head("/v0/binaries/{sha256}/content")
async def check_has_binary(
    resp: Response,
    sha256: str = Path(..., pattern="[a-fA-F0-9]{64}", description="SHA256 of entity to check"),
    ctx: context.Context = Depends(qr.ctx_without_queries),
):
    """Check if a binary exists."""
    # check user can access binary (enforce security)
    exists, source, label = binary_read.find_stream_references(ctx, sha256)

    # Set headers as we are preparing to respond but after all requests have
    # been made (to ensure the context captures everything)
    try:
        if not exists:
            raise ApiException(status_code=HTTP_404_NOT_FOUND, ref="Item not found", internal="")

        # check dispatcher still has binary
        ctx.dispatcher.has_binary(source, label, sha256)
    except HTTPException as e:
        qr.set_security_headers(ctx, resp, ex=e)
        raise

    qr.set_security_headers(ctx, resp)


@router.get(
    "/v0/binaries/{sha256}/content",
    response_class=StreamingResponse,
    responses={
        HTTP_200_OK: {
            "description": "Binary requested by ID",
            "content": {"application/octet-stream": {}},
        },
        HTTP_400_BAD_REQUEST: {"model": BaseError, "description": "Unsupported"},
    },
)
async def download_binary_encoded(
    resp: Response,
    sha256: str = Path(..., pattern="[a-fA-F0-9]{64}", description="SHA256 of entity to download"),
    ctx: context.Context = Depends(qr.ctx),
):
    """Download a binary file and cart it."""
    # check user can access binary (enforce security)
    exists, source, label = binary_read.find_stream_references(ctx, sha256)

    try:
        if not exists:
            raise ApiException(status_code=HTTP_404_NOT_FOUND, ref="Item not found", internal="")
        async_iter_content = await ctx.dispatcher.async_get_binary(source, label, sha256)
        # Disable digestors as we don't need them and they are super slow.
        packed_cart_stream = cart.async_pack_iterable(async_iter_content, auto_digests=())
        qr.set_security_headers(ctx, resp)
        return StreamingResponse(packed_cart_stream, media_type="application/octet-stream", status_code=HTTP_200_OK)
    except HTTPException as e:
        qr.set_security_headers(ctx, resp, ex=e)
        raise


@router.post(
    "/v0/binaries/content/bulk",
    response_class=StreamingResponse,
    responses={
        HTTP_200_OK: {
            "description": "Binaries requested",
            "content": {"application/octet-stream": {}},
        },
    },
)
async def download_binaries(
    request: Request,
    binaries: list[str] = Body(..., description="SHA256 of binaries to download.", embed=True),
    ctx: context.Context = Depends(qr.ctx),
):
    """Download requested binaries in a ZIP file."""
    # do simple hash lookup to check if user can access binary
    try:
        to_download = []
        for sha256 in binaries:
            exists, source, label = binary_read.find_stream_references(ctx, sha256)
            if exists:
                to_download.append((source, label, sha256))

        if not to_download:
            # Respond with a security label indicating the max security for the user's request
            raise ApiException(
                status_code=HTTP_404_NOT_FOUND,
                ref="No items found",
                internal="no items found",
            )

        success = False
        ostream = io.BytesIO()
        with pyzipper.ZipFile(ostream, mode="w", compression=pyzipper.ZIP_DEFLATED) as zf:
            for source, label, sha256 in to_download:
                try:
                    rsp = ctx.dispatcher.get_binary(source, label, sha256)
                except DispatcherApiException:
                    continue
                else:
                    data = rsp.content
                    zf.writestr(sha256, data)
                    success = True
        ostream.seek(0)

        # check that we got at least one file for the zip
        if not success:
            raise ApiException(
                status_code=HTTP_404_NOT_FOUND,
                ref="No items found",
                internal="no items found",
            )

        for _source, _label, sha256 in to_download:
            ctx.man.s.log_to_loki(ctx.user_info.username, request, sha256)

        resp = StreamingResponse(ostream, media_type="application/octet-stream", status_code=HTTP_200_OK)
        qr.set_security_headers(ctx, resp)
        return resp
    except HTTPException as e:
        qr.set_security_headers(ctx, None, ex=e)
        raise


@router.get(
    "/v0/binaries/{sha256}/content/{stream}",
    response_class=StreamingResponse,
    responses={
        HTTP_200_OK: {
            "description": "Binary requested by ID",
            "content": {"application/octet-stream": {}},
        },
        HTTP_400_BAD_REQUEST: {"model": BaseError, "description": "Unsupported"},
    },
)
async def download_binary_raw(
    resp: Response,
    sha256: str = Path(..., pattern="[a-fA-F0-9]{64}", description="SHA256 of entity containing stream"),
    stream: str = Path(..., pattern="[a-fA-F0-9]{64}", description="SHA256 of stream to download"),
    ctx: context.Context = Depends(qr.ctx),
):
    """Download a stream for a binary for permitted file types."""
    source, stream_data = binary_read.find_stream_metadata(
        ctx,
        sha256=sha256,
        stream_hash=stream,
    )

    try:
        if not source or not stream_data:
            raise ApiException(status_code=HTTP_404_NOT_FOUND, ref="Stream not found", internal="")

        attachment_type = get_attachment_type(stream_data.file_format, sha256, stream)
        if not attachment_type:
            # we currently block any streams that don't match whitelisted types
            raise ApiException(
                status_code=HTTP_400_BAD_REQUEST,
                ref="Stream file type not allowed for direct download",
                internal=f"Got: {stream_data.file_format}",
                external=f"Got: {stream_data.file_format}",
            )

        asyncIterable = await ctx.dispatcher.async_get_binary(source, stream_data.label, stream)
        qr.set_security_headers(ctx, resp)
        # nosniff: prevent browsers guessing mime types themselves
        return StreamingResponse(
            asyncIterable,
            headers={"X-Content-Type-Options": "nosniff"},
            media_type=attachment_type,
            status_code=HTTP_200_OK,
        )
    except HTTPException as e:
        qr.set_security_headers(ctx, resp, ex=e)
        raise


@router.get(
    "/v0/binaries/{sha256}/hexview",
    response_model=bedr_binaries_data.BinaryHexView,
    response_model_exclude_unset=True,
    responses={
        HTTP_200_OK: {
            "description": "Binary hex view requested",
            "content": {"application/json": {}},
        },
        HTTP_400_BAD_REQUEST: {"model": BaseError, "description": "Unsupported"},
    },
)
def get_hex_view(
    resp: Response,
    sha256: str = Path(..., pattern="[a-fA-F0-9]{64}", description="SHA256 of entity to get hexview from"),
    offset: int = Query(0, ge=0, description="Return hexview from this offset"),
    max_bytes_to_read: int | None = Query(
        None, ge=0, description="How many bytes to return, this must be set to return a range"
    ),
    shortform: bool = Query(
        False,
        description="If true, will return 16 hex bytes as a string instead of 16 strings in a list",
    ),
    ctx: context.Context = Depends(qr.ctx),
) -> bedr_binaries_data.BinaryHexView:
    """Return JSON of hex text of file requested."""

    def hex_group_formatter(iterable, _chunk_size) -> list[str]:
        """Print data bytes in readable hex."""
        # convert each byte to plaintext
        ret = [format(x, "0>2x").upper() for x in iterable]
        # Buffer to ensure always chuck_size numb. in array returned
        ret += ["" for _ in range(len(ret), _chunk_size)]
        return ret

    def hex_group_line_formatter(iterable, _chunk_size) -> str:
        """Print data bytes in readable hex."""
        # convert each byte to plaintext
        ret = [format(x, "0>2x").lower() for x in iterable]
        # group every two bytes together
        tmp = ["".join(ret[i : (i + 2)]) for i in range(0, _chunk_size, 2)]
        # separate every two bytes by a space
        return " ".join(x for x in tmp if x)

    def hex_viewer(
        stream, _offset, _chunk_size
    ) -> Generator[bedr_binaries_data.BinaryHexView.BinaryHexValue, None, None]:
        """Iterate through binary displaying as human readable hex."""
        for chunk_count in itertools.count(1):
            chunk = stream[(chunk_count - 1) * _chunk_size : (chunk_count - 1) * _chunk_size + _chunk_size]
            if not chunk:
                return
            if shortform:
                hex = hex_group_line_formatter(chunk, _chunk_size)
            else:
                hex = hex_group_formatter(chunk, _chunk_size)
            yield bedr_binaries_data.BinaryHexView.BinaryHexValue(
                address=_offset + (chunk_count - 1) * _chunk_size,
                hex=hex,
                ascii=data_hex.ascii_group_formatter(chunk),
            )

    # do simple hash lookup to check if user can access binary
    exists, source, label = binary_read.find_stream_references(ctx, sha256)

    try:
        if not exists:
            raise ApiException(status_code=HTTP_404_NOT_FOUND, ref="Item not found", internal="")

        # Get binary from dispatcher
        if max_bytes_to_read is None:
            end_byte = None
        else:
            end_byte = offset + max_bytes_to_read - 1

        rsp = ctx.dispatcher.get_binary(source=source, label=label, sha256=sha256, start_pos=offset, end_pos=end_byte)

        data = rsp.content

        # Sort out ranges
        if offset != 0 or end_byte is not None:
            total_content_length = int(rsp.headers["Content-Range"].split("/")[-1], 10)
            if end_byte is None:
                next_start_byte = total_content_length
            else:
                next_start_byte = end_byte + 1
            has_more = next_start_byte < total_content_length
            next_offset = min(total_content_length, next_start_byte)

        else:
            total_content_length = len(rsp.content)
            has_more = False
            next_offset = total_content_length

        chunk_size = 16
        values = list(hex_viewer(data, offset, chunk_size))
        if shortform:
            header = bedr_binaries_data.BinaryHexView.BinaryHexHeader(
                address="ADDRESS",
                hex=hex_group_line_formatter(range(chunk_size), chunk_size),
                ascii="ASCII",
            )
        else:
            header = bedr_binaries_data.BinaryHexView.BinaryHexHeader(
                address="ADDRESS",
                hex=hex_group_formatter(range(chunk_size), chunk_size),
                ascii="ASCII",
            )
    except HTTPException as e:
        qr.set_security_headers(ctx, resp, ex=e)
        raise

    qr.set_security_headers(ctx, resp)
    return bedr_binaries_data.BinaryHexView(
        hex_strings=values,
        header=header,
        has_more=has_more,
        next_offset=next_offset,
        content_length=total_content_length,
    )


async def _handle_search_query(
    sha256: str,
    offset: int,
    max_bytes_to_read: int | None,
    max_results: int,
    ctx: context.Context,
    provider: Callable[[bytes, int], tuple[list[bedr_binaries_data.SearchResult], int, bool]],
    is_ai_filtering: bool = False,
) -> bedr_binaries_data.BinaryStrings:
    """Generic handler that searches for content in a file with a given search engine."""
    # do simple hash lookup to check if user can access binary
    exists, source, label = binary_read.find_stream_references(ctx, sha256)
    if not exists:
        raise ApiException(status_code=HTTP_404_NOT_FOUND, ref="Item not found", internal="")

    # Get binary from dispatcher
    if max_bytes_to_read is None:
        end_byte = None
    else:
        end_byte = offset + max_bytes_to_read - 1

    async_iterable_content = await ctx.dispatcher.async_get_binary(source, label, sha256, offset, end_byte)
    strings, read_content_length, has_more = await provider(async_iterable_content, offset)
    next_offset = offset + read_content_length
    if is_ai_filtering:
        return bedr_binaries_data.BinaryStrings(
            strings=strings,
            has_more=has_more,
            next_offset=next_offset,
        )

    # Correct offset if have not taken all strings
    if len(strings) > max_results:
        has_more = True
        next_offset = strings[max_results].offset

    return bedr_binaries_data.BinaryStrings(
        strings=strings[:max_results],
        has_more=has_more,
        next_offset=next_offset,
    )


DEFAULT_MAX_BYTES_TO_READ = 10 * 1024 * 1024  # 10MB worth of strings.


@router.get(
    "/v0/binaries/{sha256}/strings",
    response_model=bedr_binaries_data.BinaryStrings,
    response_model_exclude_unset=True,
    responses={
        HTTP_200_OK: {
            "description": "Strings found in binary",
            "content": {"application/json": {}},
        },
        HTTP_400_BAD_REQUEST: {"model": BaseError, "description": "Unsupported"},
    },
)
async def get_strings(
    resp: Response,
    sha256: str = Path(..., pattern="[a-fA-F0-9]{64}", description="SHA256 of entity to get strings from"),
    min_length: int = Query(4, ge=0, description="Minimum length of string (when decoded)."),
    max_length: int = Query(200, ge=0, description="Maximum length of string (when decoded)."),
    offset: int = Query(0, ge=0, description="Search for strings from offset"),
    max_bytes_to_read: int = Query(
        DEFAULT_MAX_BYTES_TO_READ,
        ge=0,
        description="How many bytes to search for, if this is not set, "
        + "returns 10MB of the file, if it set to larger than the file the whole file will be searched.",
    ),
    take_n_strings: int = Query(1000, ge=0, description="How many strings to return"),
    filter: str | None = Query(None, description="Case-insensitive search string to filter strings with"),
    regex: str | None = Query(None, description="Regex pattern to search strings with"),
    ctx: context.Context = Depends(qr.ctx),
    file_format: str | None = Query(None, description="Optional file type for AI string filter."),
) -> bedr_binaries_data.BinaryStrings:
    """Return strings found in the binary.

    Looks for ASCII, UTF-16 and UTF-32 big and little endian strings.
    """
    # Filter is normally shadowed
    exported_find_filter = filter
    exported_regex = None

    try:
        if regex is not None:
            exported_regex = re.compile(regex)
    except Exception:
        raise ApiException(status_code=HTTP_400_BAD_REQUEST, ref="Invalid regex pattern", internal="")

    # If AI string filter is enabled; call it to add file type labels to strings
    s = settings.get()

    is_ai_filtering = False
    if file_format is not None and s.smart_string_filter_url:
        logger.info("eligible string filter is online")
        is_ai_filtering = True

    async def search_handler(
        data_stream: AsyncIterable[bytes],
        current_offset: int,
    ) -> tuple[list[bedr_binaries_data.SearchResult], int, bool]:
        """Take an async stream and consume it until until the strings limit is reached.

        Returns: tuple with strings found, total data read from the file and a boolean indicating if the end of the
        file was reached.
        """
        strings, read_content_length, has_more_content = await data_strings.get_strings(
            data_stream, min_length, max_length, current_offset, exported_find_filter, exported_regex, take_n_strings
        )
        # Sort list so next n strings are correct
        strings.sort(key=lambda x: x.offset)
        return strings, read_content_length, has_more_content

    try:
        search = await _handle_search_query(
            sha256, offset, max_bytes_to_read, take_n_strings, ctx, search_handler, is_ai_filtering
        )
    except HTTPException as e:
        qr.set_security_headers(ctx, resp, ex=e)
        raise

    qr.set_security_headers(ctx, resp)

    # timeout field for if ai string filter takes too long to process
    time_out = False

    if file_format is not None and s.smart_string_filter_url:
        while True:
            last_processed_offset = 0
            batch_size = 2000
            ai_filtered_strings = []
            MAX_STRINGS_TO_FIND = 20
            TIME_OUT = 30  # 30 second timeout
            start_time = time.time()

            for i in range(0, len(search.strings), batch_size):
                try:
                    current_batch_size = min(batch_size, len(search.strings) - i)
                    batch = search.strings[i : i + current_batch_size]  # Slice the list into batches
                    # call the AI string filter pod
                    response = string_filter.call_string_filter(file_format, batch, s.smart_string_filter_url)
                    if response is not None:
                        ai_filtered_strings.extend(response.json())
                    safe_index = i + min(batch_size, len(search.strings) - i) - 1
                    last_processed_offset = search.strings[safe_index].offset
                    if len(ai_filtered_strings) >= MAX_STRINGS_TO_FIND:
                        break
                    if (time.time() - start_time) > TIME_OUT:
                        time_out = True
                        break
                except HTTPException as e:
                    qr.set_security_headers(ctx, resp, ex=e)
                    raise
            next_offset = 0

            if last_processed_offset < search.strings[-1].offset:
                has_more = True

                if ai_filtered_strings:
                    last_offset = ai_filtered_strings[-1]["offset"]
                else:
                    has_more = False
                    break

                # calculate the next offset if has_more
                index = next(
                    (i for i, item in enumerate(search.strings) if item.offset == last_offset),
                    -1,
                )

                if index == len(search.strings) - 1:
                    has_more = False
                    break

                if index != -1:
                    next_offset = search.strings[index + 1].offset
            else:
                has_more = False

            # Updating search.strings with filtered results
            search.strings = string_filter.filter_search_results(search.strings, ai_filtered_strings)

            if len(search.strings) == 0:
                break

            search.has_more = has_more
            search.next_offset = next_offset
            search.strings.sort(key=lambda x: x.offset)
            break
    search.time_out = time_out
    return search


@router.get(
    "/v0/binaries/{sha256}/search/hex",
    response_model=bedr_binaries_data.BinaryStrings,
    response_model_exclude_unset=True,
    responses={
        HTTP_200_OK: {
            "description": "Hex matches found in binary",
            "content": {"application/json": {}},
        },
        HTTP_400_BAD_REQUEST: {"model": BaseError, "description": "Unsupported"},
    },
)
async def search_hex(
    resp: Response,
    sha256: str = Path(..., pattern="[a-fA-F0-9]{64}", description="SHA256 of entity to get strings from"),
    offset: int = Query(0, ge=0, description="Search for hits from offset"),
    max_bytes_to_read: int | None = Query(
        None, ge=0, description="How many bytes to search for, if this is not set, return to EOF"
    ),
    take_n_hits: int = Query(1000, ge=0, description="How many hits to return"),
    filter: str = Query(description="Hex filter to apply to the file"),
    ctx: context.Context = Depends(qr.ctx),
) -> bedr_binaries_data.BinaryStrings:
    """Return hits found in the binary for a specific hex pattern."""
    exported_filter = filter

    async def search_handler(
        data_stream: AsyncIterable[bytes], current_offset: int
    ) -> tuple[list[bedr_binaries_data.SearchResult], int, bool]:
        """Take an async stream and consume it searching for a hex pattern and returning hits up to the limit.

        Returns: tuple with hex found, total data read from the file and a boolean indicating if the end of the
        file was reached.
        """
        filter = exported_filter.strip().upper()
        # Convert into a raw pattern
        # This will always be hex, but we need to allow for an 0x prefix
        if filter.startswith("0x") or filter.startswith("0X"):
            filter = filter[2:]

        try:
            filter_bytes = bytes(bytearray.fromhex(filter))
        except Exception:
            raise ApiException(status_code=HTTP_400_BAD_REQUEST, ref="Invalid hex pattern", internal="")

        return await data_hex.get_hex_hits(data_stream, current_offset, take_n_hits, filter_bytes)

    try:
        search = await _handle_search_query(sha256, offset, max_bytes_to_read, take_n_hits, ctx, search_handler)
    except HTTPException as e:
        qr.set_security_headers(ctx, resp, ex=e)
        raise

    qr.set_security_headers(ctx, resp)
    return search
