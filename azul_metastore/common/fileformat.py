"""Handle decompress/extract/decode different file types."""

import io
import logging
import zipfile
from typing import AsyncIterable, Iterable, Optional, Tuple

import malpz
import pyzipper
from azul_bedrock.dispatcher import DispatcherAPI
from azul_bedrock.exceptions import ApiException
from cart import cart
from fastapi import UploadFile
from pydantic import ByteSize
from starlette.status import HTTP_422_UNPROCESSABLE_CONTENT

logger = logging.getLogger(__name__)

# Max bytes before extraction of a bundled submission
MAX_BUNDLED_PRE_EXTRACT_SIZE = 1024 * 1024 * 100
# Max bytes after extracting a bundled submisssion
MAX_BUNDLED_POST_EXTRACT_SIZE = MAX_BUNDLED_PRE_EXTRACT_SIZE * 2
# Max number of files in a bundled submission
MAX_BUNDLED_FILES = 1000
# Max characters in directories + filename for each file in bundled submission
MAX_BUNDLED_FILENAME_LENGTH = 255


class ExtractException(Exception):
    """Supplied file could not be extracted."""

    pass


async def unpack_content(
    binary: UploadFile, extract: bool = False, password: str = None
) -> AsyncIterable[Tuple[AsyncIterable[bytes] | UploadFile, Optional[str]]]:
    """Unpack available binary samples from supplied input stream.

    If `extract` is True the input stream will be treated as an archive format and
    attempt to extract children within.  Currently supports: tar/gzip/zip.

    If `password` is set it will be supplied to archive formats for extraction.

    If the input stream is a supported neutering format, it will be stripped.
    Currently supports: cart/malpz.

    Where filenames are supported in extracted streams, these will be yielded
    with their content, otherwise as None.
    """
    items = [(binary, None)]

    if extract:
        # Check if the file metadata says it's too big.
        too_big_fail = True
        if binary.size and binary.size > MAX_BUNDLED_PRE_EXTRACT_SIZE:
            too_big_fail = True
        else:
            # Metadata doesn't say the file is too big so buffer the file to confirm that's true.
            too_big_fail = False
            buffered_data = await binary.read(MAX_BUNDLED_PRE_EXTRACT_SIZE)

        if too_big_fail or len(buffered_data) == MAX_BUNDLED_PRE_EXTRACT_SIZE:
            raise ApiException(
                status_code=HTTP_422_UNPROCESSABLE_CONTENT,
                ref=f"Can't extract file larger than {ByteSize(MAX_BUNDLED_PRE_EXTRACT_SIZE).human_readable()}",
                external=f"Can't extract file larger than {ByteSize(MAX_BUNDLED_PRE_EXTRACT_SIZE).human_readable()}",
                internal="extract_file_too_large",
            )
        # if submitter trusts the file enough to be bundled submission, it shouldn't be cart'd so we don't uncart here
        data_to_extract = io.BytesIO(buffered_data)
        items = extract_archive(data_to_extract, password=password)

    for raw_upload_file, fname in items:
        fname2 = None
        data, fname2 = await async_extract_neutered(raw_upload_file)
        if data:
            # Extracted from neutering so prefer extracted name
            yield data, fname2 or fname
        else:
            # Not extracted from neutering
            yield raw_upload_file, fname


async def async_extract_neutered(upload_file: UploadFile) -> tuple[AsyncIterable[bytes] | None, str | None]:
    """Handle extracting MalPZ/CaRT."""
    fname = None
    try:
        async_iter = DispatcherAPI.async_convert_to_async_iterable(upload_file)
        async_iter_data, meta = await cart.async_unpack_iterable(async_iter)
        fname = meta.get("name")
        return async_iter_data, fname
    except cart.InvalidCARTException:
        pass

    # Seek back to 0 if cart was unsuccessful at any point.
    await upload_file.seek(0)
    data = None
    try:
        data, _ = await async_handle_malpz(upload_file)
        return DispatcherAPI.async_convert_to_async_iterable(data), fname
    except malpz.MetadataException:
        pass
    return data, fname


def extract_archive(istream: io.BytesIO, password: str = None) -> Iterable[Tuple[UploadFile, Optional[str]]]:
    """Handle extracting data from zip archive file format."""
    extracted = False
    password = password.encode("utf8") if password else None
    istream.seek(0)
    try:
        with pyzipper.AESZipFile(istream) as zpf:
            zpf.setpassword(password)
            namelist = list(zpf.namelist())
            if len(namelist) > MAX_BUNDLED_FILES:
                raise ExtractException(f"too many files ({len(namelist)}/{MAX_BUNDLED_FILES})")
            # check total extracted size
            total_bytes = 0
            for n in namelist:
                info: zipfile.ZipInfo = zpf.getinfo(n)
                total_bytes += info.file_size
            if total_bytes > MAX_BUNDLED_POST_EXTRACT_SIZE:
                raise ExtractException(
                    f"too many bytes in extracted submission ({total_bytes}/{MAX_BUNDLED_POST_EXTRACT_SIZE})"
                )
            # get bytes for each file in zip
            for n in namelist:
                info: zipfile.ZipInfo = zpf.getinfo(n)
                if info.is_dir():
                    continue
                if len(n) > MAX_BUNDLED_FILENAME_LENGTH:
                    raise ExtractException(
                        f"file path too long ({len(n)}/{MAX_BUNDLED_FILENAME_LENGTH}): "
                        f"{n[:MAX_BUNDLED_FILENAME_LENGTH]}..."
                    )
                if "/../" in n:
                    raise ExtractException(f"file name has bad character sequence: {n}")
                if len(zpf.read(n)) == 0:
                    continue
                extracted = True
                yield UploadFile(io.BytesIO(zpf.read(n))), n
    except pyzipper.BadZipFile as e:
        if "File is not a zip file" in str(e):
            raise ExtractException("not a zip file")
        else:
            # unknown error
            raise ExtractException(str(type(e)) + " " + str(e))
    except RuntimeError as e:
        if "password required for extraction" in str(e):
            raise ExtractException("zip requires password")
        elif "Bad password for file" in str(e):
            raise ExtractException("bad zip password")
        else:
            # unknown error
            raise ExtractException(str(type(e)) + " " + str(e))
    if not extracted:
        raise ExtractException("no files were extracted from zip")


async def async_handle_malpz(async_stream: UploadFile) -> Tuple[bytes, dict]:
    """Unwrap MalPZ file.

    Raises an exception if the provided stream is not a malpz stream.
    :param istream: MalPZ input file-like object
    :return: unpacked data, unpacked meta
    """
    try:
        potential_malpz_header = await async_stream.read(len(malpz.MALPZ_HEADER))
        # Exception will raise if it's not a malpz file.
        malpz.validate_version(potential_malpz_header)
        buffered_data = potential_malpz_header + await async_stream.read(
            MAX_BUNDLED_PRE_EXTRACT_SIZE - len(malpz.MALPZ_HEADER)
        )
        if len(buffered_data) >= MAX_BUNDLED_PRE_EXTRACT_SIZE:
            raise ApiException(
                status_code=HTTP_422_UNPROCESSABLE_CONTENT,
                ref=f"Can't unmalpz file larger than {ByteSize(MAX_BUNDLED_PRE_EXTRACT_SIZE).human_readable()}",
                external=f"Can't unmalpz file larger than {ByteSize(MAX_BUNDLED_PRE_EXTRACT_SIZE).human_readable()}",
                internal="unmalpz_file_too_large",
            )
        unwrapped = malpz.unwrap(buffered_data)
        meta = unwrapped.get("meta", {})
        return unwrapped["data"], meta
    except Exception:
        await async_stream.seek(0)
        raise


# Legacy stream types that are permitted
LEGACY_PERMITTED_TEXT_TYPES = ["text", "javascript", "powershell", "vba", "batch"]

LEGACY_PERMITTED_IMAGE_TYPES = ["PNG", "JPG", "BMP", "ICO"]


def get_attachment_type(
    file_format_legacy: str, file_format: Optional[str], entity_hash: str, request_hash: str
) -> Optional[str]:
    """Determines if the specified stream should be permitted to be streamed to a client.

    If a filetype should be streamed, a HTTP MIME type will be returned.
    """
    # Scan for filetypes that are always permitted
    not_null_type = file_format.lower() if file_format is not None else ""

    # Text/code types
    if (
        not_null_type.startswith("code/")
        or not_null_type.startswith("text/")
        or not_null_type == "application/json"
        or not_null_type == "application/javascript"
        or file_format_legacy.lower() in LEGACY_PERMITTED_TEXT_TYPES
    ):
        # This is a text file
        return "text/plain"

    # Determine if this attachment is a input file (untrusted) or an altstream (derived
    # from a plugin)
    if entity_hash.lower() == request_hash.lower():
        # This is a input file request
        # Only permit text files
        return None

    # Additional types that a client could reasonably parse if this is an alt stream
    if not_null_type == "application/vnd.tcpdump.pcap":
        return "application/vnd.tcpdump.pcap"

    if not_null_type == "document/pdf":
        return "application/pdf"

    if not_null_type.startswith("image/"):
        # Use the identified type as this lines up with regular mime types
        return not_null_type

    return None
