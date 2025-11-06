import respx

from tests.support import unit_test
from tests.unit.data.helpers import mock_load_binary_async_iterable_content


class TestMain(unit_test.DataMockingUnitTest):
    @respx.mock
    def test_search_hex(self):
        sha256a = "1d4ce380c90339830b7915137b34cd77c627ad498c773f9b4a12a3c876bfe024"
        # FUTURE use a standard client mock
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(
            side_effect=mock_load_binary_async_iterable_content
        )

        # Test with a generic ASCII filter
        response = self.client.get(f"/v0/binaries/{sha256a}/search/hex", params={"filter": "393833"})
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 359,
            "strings": [{"string": "983", "length": 3, "offset": 349, "encoding": "hex"}],
        }
        self.assertEqual(expected_result, response.json())
        self.assertEqual(
            response.headers.get("x-azul-security"), "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"
        )

        # Test for a character that should be escaped
        response = self.client.get(f"/v0/binaries/{sha256a}/search/hex", params={"filter": "09"})
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 359,
            "strings": [
                {"string": "\\x09", "length": 1, "offset": 65, "encoding": "hex"},
                {"string": "\\x09", "length": 1, "offset": 105, "encoding": "hex"},
            ],
        }
        self.assertEqual(expected_result, response.json())

        # Test for bad input (not 2 digit hex characters)
        response = self.client.get(f"/v0/binaries/{sha256a}/search/hex", params={"filter": "123"})
        # Bad request
        self.assertEqual(400, response.status_code)
        self.assertEqual(
            response.headers.get("x-azul-security"), "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"
        )

    @respx.mock
    def test_different_input(self):
        sha256a = "0ca0bafc3d3fd6960c7f7bc6c63064279746bab14ea7aaed735f43049c9627dc"
        # FUTURE use a standard client mock
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(
            side_effect=mock_load_binary_async_iterable_content
        )

        # Test with spaces between the characters
        response = self.client.get(f"/v0/binaries/{sha256a}/search/hex", params={"filter": "730066006100"})
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 224,
            "strings": [{"string": "s\\x00f\\x00a\\x00", "length": 6, "offset": 2, "encoding": "hex"}],
        }
        self.assertEqual(expected_result, response.json())

        # Test with 0x at the beginning between the characters (this should still parse as 73 00 66 00 61 00)
        response = self.client.get(f"/v0/binaries/{sha256a}/search/hex", params={"filter": "0x730066006100"})
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 224,
            "strings": [{"string": "s\\x00f\\x00a\\x00", "length": 6, "offset": 2, "encoding": "hex"}],
        }
        self.assertEqual(expected_result, response.json())

        # Test with leading/trailing whitespace
        response = self.client.get(f"/v0/binaries/{sha256a}/search/hex", params={"filter": "   73 00  6600 61 00   "})
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 224,
            "strings": [{"string": "s\\x00f\\x00a\\x00", "length": 6, "offset": 2, "encoding": "hex"}],
        }
        self.assertEqual(expected_result, response.json())

        # Test with invalid characters
        response = self.client.get(f"/v0/binaries/{sha256a}/search/hex", params={"filter": "Hello, World!"})
        self.assertEqual(400, response.status_code)
