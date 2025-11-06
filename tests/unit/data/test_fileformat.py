import io
from typing import AsyncIterable

from azul_bedrock.test_utils import file_manager
from fastapi import UploadFile

from azul_metastore.common import fileformat
from tests.support import unit_test

from . import helpers


class FileformatTestCases(unit_test.BaseUnitTestCase):
    maxDiff = None

    async def _convert_to_list(
        self, val: AsyncIterable[tuple[AsyncIterable[bytes] | UploadFile, str | None]]
    ) -> list[tuple[AsyncIterable[bytes] | UploadFile, str | None]]:
        """Convert an asyncIterable to a list"""
        return [d async for d in val]

    async def _convert_to_bytes(self, val: AsyncIterable) -> bytes:
        """Convert an asyncIterable of bytes to bytes."""
        if isinstance(val, UploadFile):
            return await val.read()
        data = b""
        async for x in val:
            data += x
        return data

    async def test_extract_archive(self):
        # zip, no password, 1 file
        fm = file_manager.FileManager()
        with helpers.get_file("fake.zip") as indata:
            out = list(fileformat.extract_archive(indata))
            self.assertEqual(1, len(out))
            d, name = out[0]
            self.assertEqual("fake.txt", name)
            self.assertEqual(121, len(await d.read()))

        # zip, correct password, 2 files
        with helpers.get_file("fake_infected.zip") as indata:
            out = list(fileformat.extract_archive(indata, "infected"))
            self.assertEqual(2, len(out))
            d, name = out[0]
            self.assertEqual("fake.txt", name)
            self.assertEqual(121, len(await d.read()))
            d, name = out[1]
            self.assertEqual("fake_meta.txt", name)
            self.assertEqual(204, len(await d.read()))

        # zip, missing password
        with helpers.get_file("fake_infected.zip") as indata:
            with self.assertRaisesRegex(fileformat.ExtractException, r"zip requires password"):
                list(fileformat.extract_archive(indata))

        # zip, wrong password
        with helpers.get_file("fake_infected.zip") as indata:
            with self.assertRaisesRegex(fileformat.ExtractException, r"bad zip password"):
                list(fileformat.extract_archive(indata, "hello"))

        # zip, bomb to 5gb
        indata = io.BytesIO(fm.download_file_bytes("fb4ff972d21189beec11e05109c4354d0cd6d3b629263d6c950cf8cc3f78bd99"))
        with self.assertRaisesRegex(fileformat.ExtractException, r"too many bytes in extracted submission"):
            list(fileformat.extract_archive(indata, "hello"))

        # non-archive/unsupported file type
        with helpers.get_file("fake.txt") as indata:
            with self.assertRaisesRegex(fileformat.ExtractException, r"not a zip file"):
                list(fileformat.extract_archive(indata))

        # too long file names
        with helpers.get_file("harden_long_files.zip") as indata:
            with self.assertRaisesRegex(fileformat.ExtractException, r"file path too long"):
                list(fileformat.extract_archive(indata))

        # too many files
        with helpers.get_file("harden_too_many_files.zip") as indata:
            with self.assertRaisesRegex(fileformat.ExtractException, r"too many files"):
                list(fileformat.extract_archive(indata))

    async def test_handle_malpz(self):
        with helpers.get_file("fake.txt.malpz") as indata:
            d, meta = await fileformat.async_handle_malpz(UploadFile(indata))
            self.assertEqual(121, len(d))
            self.assertEqual({}, meta)

    async def test_extract_neutered(self):
        # malpz
        with helpers.get_file("fake.txt.malpz") as indata:
            d, fname = await fileformat.async_extract_neutered(UploadFile(indata))
            self.assertEqual(121, len(await self._convert_to_bytes(d)))
        # cart
        with helpers.get_file("fake.txt.cart") as indata:
            d, fname = await fileformat.async_extract_neutered(UploadFile(indata))
            self.assertEqual(121, len(await self._convert_to_bytes(d)))
            self.assertEqual("fake.txt", fname)
        # unsupported neutering
        with helpers.get_file("fake.zip") as indata:
            d, fname = await fileformat.async_extract_neutered(UploadFile(indata))
            self.assertTrue(d is None)

    async def test_unpack_content(self):
        # wrapping func to handle all formats.

        # malpz
        with helpers.get_file("fake.txt.malpz") as indata:
            file_to_upload = UploadFile(indata)
            out = await self._convert_to_list(fileformat.unpack_content(file_to_upload))
            self.assertEqual(1, len(out))
            d, fname = out[0]
            self.assertEqual(121, len(await self._convert_to_bytes(d)))
            # discards any neutering format metadata including filenames
            self.assertTrue(fname is None)

            # Verify the raw file without unmalpzing is the expected size
            await file_to_upload.seek(0)
            self.assertEqual(202, len(await file_to_upload.read()))

        # cart
        with helpers.get_file("fake.txt.cart") as indata:
            out = await self._convert_to_list(fileformat.unpack_content(UploadFile(indata)))
            self.assertEqual(1, len(out))
            d, fname = out[0]
            self.assertEqual(121, len(await self._convert_to_bytes(d)))
            self.assertEqual("fake.txt", fname)

        # zip without extraction
        with helpers.get_file("fake.zip") as indata:
            out = await self._convert_to_list(fileformat.unpack_content(UploadFile(indata)))
            self.assertEqual(1, len(out))
            d, fname = out[0]
            self.assertEqual(190, len(await self._convert_to_bytes(d)))
            self.assertTrue(fname is None)

        # zip with extraction
        with helpers.get_file("fake.zip") as indata:
            out = await self._convert_to_list(fileformat.unpack_content(UploadFile(indata), extract=True))
            self.assertEqual(1, len(out))
            d, fname = out[0]
            self.assertEqual(121, len(await self._convert_to_bytes(d)))
            self.assertEqual("fake.txt", fname)

        # zip with extraction of cart
        with helpers.get_file("zipped.cart.zip") as indata:
            out = await self._convert_to_list(fileformat.unpack_content(UploadFile(indata), extract=True))
            self.assertEqual(1, len(out))
            d, fname = out[0]
            self.assertEqual(121, len(await self._convert_to_bytes(d)))
            self.assertEqual("fake.txt", fname)

        # carted zip extraction
        with helpers.get_file("cartedzipfaketext.zip.cart") as indata:
            with self.assertRaises(fileformat.ExtractException):
                await self._convert_to_list(fileformat.unpack_content(UploadFile(indata), extract=True))

        # malpz'd zip extraction
        with helpers.get_file("cartedzipfaketext.zip.malpz") as indata:
            with self.assertRaises(fileformat.ExtractException):
                await self._convert_to_list(fileformat.unpack_content(UploadFile(indata), extract=True))

        # text file
        with helpers.get_file("fake.txt") as indata:
            with self.assertRaises(fileformat.ExtractException):
                await self._convert_to_list(fileformat.unpack_content(UploadFile(indata), extract=True))

        # text file - can't extract
        with helpers.get_file("fake.txt") as indata:
            with self.assertRaises(fileformat.ExtractException):
                await self._convert_to_list(fileformat.unpack_content(UploadFile(indata), extract=True))
