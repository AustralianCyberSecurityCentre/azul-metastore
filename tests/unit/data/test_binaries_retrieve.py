import io
from unittest import mock

import cart
import httpx
import pyzipper
import respx
from azul_bedrock import models_network as azm

from tests.support import unit_test


def unpack(content):
    """Uncart a bytes stream."""
    raw = io.BytesIO()
    cart.unpack_stream(io.BytesIO(content), raw)
    return raw.getvalue()


class TestMain(unit_test.DataMockingUnitTest):
    @respx.mock
    def test_head_binaries_sha256_content(self):
        sha256a = "e10fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        sha256b = "e100ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        respx.head(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(httpx.Response(status_code=200))
        respx.head(f"{self.end}/api/v3/stream/source/label/{sha256b}").mock(httpx.Response(status_code=404))

        response = self.client.head(f"/v0/binaries/{sha256a}/content")
        self.assertEqual(200, response.status_code)
        self.assertEqual(
            response.headers.get("x-azul-security"), "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"
        )
        response = self.client.head(f"/v0/binaries/{sha256b}/content")
        self.assertEqual(404, response.status_code)
        self.assertEqual(
            response.headers.get("x-azul-security"), "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"
        )

        # check case sensitivity
        response = self.client.head(f"/v0/binaries/{sha256a.upper()}/content")
        self.assertEqual(200, response.status_code)
        response = self.client.head(f"/v0/binaries/{sha256b.upper()}/content")
        self.assertEqual(404, response.status_code)

    @respx.mock
    def test_get_binaries_sha256_content(self):
        content = b"hello"
        sha256a = "e10fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        sha256b = "e100ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(
            httpx.Response(status_code=200, content=content)
        )
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256b}").mock(httpx.Response(status_code=404))

        response = self.client.get(f"/v0/binaries/{sha256a}/content")
        self.assertEqual(200, response.status_code)
        self.assertEqual(content, unpack(response.content))
        response = self.client.get(f"/v0/binaries/{sha256b}/content")
        self.assertEqual(404, response.status_code)

        # check case sensitivity
        response = self.client.get(f"/v0/binaries/{sha256a.upper()}/content")
        self.assertEqual(200, response.status_code)
        self.assertEqual(content, unpack(response.content))
        response = self.client.get(f"/v0/binaries/{sha256b.upper()}/content")
        self.assertEqual(404, response.status_code)

    @respx.mock
    # @mock.patch('azul_metastore.find_stream_references', lambda *args: True)
    @mock.patch("azul_metastore.query.binary2.binary_read.find_stream_metadata")
    def test_get_binaries_sha256_content_stream(self, fs):
        content = b"hello"
        sha256a = "e10fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        sha256b = "e100ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        respx.get(f"{self.end}/api/v3/stream/source/{azm.DataLabel.TEST}/{sha256a}").mock(
            return_value=httpx.Response(status_code=422)
        )
        respx.get(f"{self.end}/api/v3/stream/source/{azm.DataLabel.TEST}/{sha256b}").mock(
            return_value=httpx.Response(status_code=200, content=content)
        )

        fs.return_value = (
            "source",
            azm.Datastream(
                label=azm.DataLabel.TEST,
                magic="",
                mime="",
                file_format="image/gif",
                size=1,
            ),
        )

        response = self.client.get(f"/v0/binaries/{sha256a}/content/{sha256b}")
        self.assertEqual(200, response.status_code)
        self.assertEqual(content, response.content)
        response = self.client.get(f"/v0/binaries/{sha256b}/content/{sha256a}")
        self.assertEqual(422, response.status_code)

        # check case sensitivity
        response = self.client.get(f"/v0/binaries/{sha256a.upper()}/content/{sha256b.upper()}")
        self.assertEqual(200, response.status_code)
        self.assertEqual(content, response.content)
        response = self.client.get(f"/v0/binaries/{sha256b.upper()}/content/{sha256a.upper()}")
        self.assertEqual(422, response.status_code)

        # bad type
        fs.return_value = (
            "source",
            azm.Datastream(
                label=azm.DataLabel.TEST,
                magic="",
                mime="",
                file_format="executable/windows/pe32",
                size=1,
            ),
        )
        response = self.client.get(f"/v0/binaries/{sha256a}/content/{sha256b}")
        self.assertEqual(400, response.status_code)
        # content stream
        fs.return_value = (
            "source",
            azm.Datastream(
                label=azm.DataLabel.TEST,
                magic="",
                mime="",
                file_format="image/gif",
                size=1,
            ),
        )
        response = self.client.get(f"/v0/binaries/{sha256b}/content/{sha256b}")
        self.assertEqual(400, response.status_code)

        # content stream, lower case filetype
        fs.return_value = (
            "source",
            azm.Datastream(
                label=azm.DataLabel.TEST,
                magic="",
                mime="",
                file_format="text/plain",
                size=1,
            ),
        )
        response = self.client.get(f"/v0/binaries/{sha256b}/content/{sha256b}")
        self.assertEqual(200, response.status_code)
        # content stream, capital case filetype
        fs.return_value = (
            "source",
            azm.Datastream(
                label=azm.DataLabel.TEST,
                magic="",
                mime="",
                file_format="Text/Plain",
                size=1,
            ),
        )
        response = self.client.get(f"/v0/binaries/{sha256b}/content/{sha256b}")
        self.assertEqual(200, response.status_code)
        # content stream, upper case filetype
        fs.return_value = (
            "source",
            azm.Datastream(
                label=azm.DataLabel.TEST,
                magic="",
                mime="",
                file_format="TEXT/PLAIN",
                size=1,
            ),
        )
        response = self.client.get(f"/v0/binaries/{sha256b}/content/{sha256b}")
        self.assertEqual(200, response.status_code)

    @respx.mock
    # @mock.patch('azul_metastore.find_stream')
    def test_get_binaries_content_bulk(self):
        body = b"hello"
        sha256a = "e10fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        sha256b = "e100ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(
            return_value=httpx.Response(status_code=200, content=body)
        )
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256b}").mock(
            return_value=httpx.Response(status_code=200, content=body)
        )

        response = self.client.post("/v0/binaries/content/bulk", json={"binaries": [sha256a, sha256b]})
        self.assertEqual(200, response.status_code)
        istream = io.BytesIO(response.content)
        self.assertTrue(pyzipper.is_zipfile(istream))
        zf = pyzipper.ZipFile(istream, "r")
        self.assertEqual([sha256a, sha256b], zf.namelist())
        self.assertEqual(body, zf.read(sha256a))
        self.assertEqual(body, zf.read(sha256b))

        response = self.client.post("/v0/binaries/content/bulk", json={"binaries": [sha256a]})
        self.assertEqual(200, response.status_code)
        istream = io.BytesIO(response.content)
        self.assertTrue(pyzipper.is_zipfile(istream))
        zf = pyzipper.ZipFile(istream, "r")
        self.assertEqual([sha256a], zf.namelist())
        self.assertEqual(body, zf.read(sha256a))

        # check case sensitivity
        response = self.client.post(
            "/v0/binaries/content/bulk",
            json={"binaries": [sha256a.upper(), sha256b.upper()]},
        )
        self.assertEqual(200, response.status_code)
        istream = io.BytesIO(response.content)
        self.assertTrue(pyzipper.is_zipfile(istream))
        zf = pyzipper.ZipFile(istream, "r")
        self.assertEqual([sha256a.upper(), sha256b.upper()], zf.namelist())
        self.assertEqual(body, zf.read(sha256a.upper()))
        self.assertEqual(body, zf.read(sha256b.upper()))

        # check missing
        response = self.client.post("/v0/binaries/content/bulk", json={"binaries": ["grthnrthtrt"]})
        self.assertEqual(422, response.status_code)

        response = self.client.post(
            "/v0/binaries/content/bulk",
            json={"binaries": [sha256a, "aaaaaabbbbffffffffffffffffffffffffffffffffffffffffffffffffffffff"]},
        )
        self.assertEqual(200, response.status_code)
        istream = io.BytesIO(response.content)
        self.assertTrue(pyzipper.is_zipfile(istream))
        zf = pyzipper.ZipFile(istream, "r")
        self.assertEqual([sha256a], zf.namelist())
        self.assertEqual(body, zf.read(sha256a))
