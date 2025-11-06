import json
import re
from unittest import mock

import respx

from tests.support import unit_test
from tests.unit.data.helpers import mock_load_binary


class TestMain(unit_test.DataMockingUnitTest):
    @respx.mock
    def test_get_hex(self):
        sha256a = "1d4ce380c90339830b7915137b34cd77c627ad498c773f9b4a12a3c876bfe024"
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(side_effect=mock_load_binary)

        response = self.client.get(f"/v0/binaries/{sha256a}/hexview", params={"offset": 0, "shortform": True})
        self.assertEqual(200, response.status_code, str(response.content))
        content = json.loads(response.content.decode("utf-8"))
        # Only compare some of result ( too big)
        content["hex_strings"] = content["hex_strings"][:5] + content["hex_strings"][-5:]

        expected_result = {
            "hex_strings": [
                {
                    "address": 0,
                    "hex": "c385 2312 c389 0ac3 aec2 a475 5f3d c394",
                    "ascii": "..#........u_=..",
                },
                {
                    "address": 16,
                    "hex": "c2ac c3a5 c394 c2b8 c384 16c2 b5c3 a8c3",
                    "ascii": "................",
                },
                {
                    "address": 32,
                    "hex": "905a c2a1 c280 c3a7 3b45 1a39 c28c c3a3",
                    "ascii": ".Z......;E.9....",
                },
                {
                    "address": 48,
                    "hex": "c281 c284 73c3 9cc2 8b32 1972 32c2 883e",
                    "ascii": "....s....2.r2..>",
                },
                {
                    "address": 64,
                    "hex": "7b09 c3b2 c2a0 c3a6 c394 c280 c38e c3a0",
                    "ascii": "{...............",
                },
                {
                    "address": 288,
                    "hex": "c29d 4cc2 b85f 0262 c386 c289 c29b c283",
                    "ascii": "..L.._.b........",
                },
                {
                    "address": 304,
                    "hex": "c2bb c393 3870 c2b0 0a68 656c 6c6f 776f",
                    "ascii": "....8p...hellowo",
                },
                {
                    "address": 320,
                    "hex": "726c 640a 7777 772e 676f 6f67 6c65 2e63",
                    "ascii": "rld.www.google.c",
                },
                {
                    "address": 336,
                    "hex": "6f6d 0a31 302e 372e 352e 373a 3339 3833",
                    "ascii": "om.10.7.5.7:3983",
                },
                {"address": 352, "hex": "0a61 7364 666c 6b", "ascii": ".asdflk"},
            ],
            "header": {
                "address": "ADDRESS",
                "hex": "0001 0203 0405 0607 0809 0a0b 0c0d 0e0f",
                "ascii": "ASCII",
            },
            "has_more": False,
            "next_offset": 359,
            "content_length": 359,
        }
        self.assertEqual(expected_result, content)
        self.assertEqual(
            response.headers.get("x-azul-security"), "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"
        )

        # check case sensitivity
        response = self.client.get(
            f"/v0/binaries/{sha256a.upper()}/hexview",
            params={"offset": 0, "shortform": True},
        )
        self.assertEqual(200, response.status_code)
        content = json.loads(response.content.decode("utf-8"))
        # Only compare some of result ( too big)
        content["hex_strings"] = content["hex_strings"][:5] + content["hex_strings"][-5:]

        expected_result = {
            "hex_strings": [
                {
                    "address": 0,
                    "hex": "c385 2312 c389 0ac3 aec2 a475 5f3d c394",
                    "ascii": "..#........u_=..",
                },
                {
                    "address": 16,
                    "hex": "c2ac c3a5 c394 c2b8 c384 16c2 b5c3 a8c3",
                    "ascii": "................",
                },
                {
                    "address": 32,
                    "hex": "905a c2a1 c280 c3a7 3b45 1a39 c28c c3a3",
                    "ascii": ".Z......;E.9....",
                },
                {
                    "address": 48,
                    "hex": "c281 c284 73c3 9cc2 8b32 1972 32c2 883e",
                    "ascii": "....s....2.r2..>",
                },
                {
                    "address": 64,
                    "hex": "7b09 c3b2 c2a0 c3a6 c394 c280 c38e c3a0",
                    "ascii": "{...............",
                },
                {
                    "address": 288,
                    "hex": "c29d 4cc2 b85f 0262 c386 c289 c29b c283",
                    "ascii": "..L.._.b........",
                },
                {
                    "address": 304,
                    "hex": "c2bb c393 3870 c2b0 0a68 656c 6c6f 776f",
                    "ascii": "....8p...hellowo",
                },
                {
                    "address": 320,
                    "hex": "726c 640a 7777 772e 676f 6f67 6c65 2e63",
                    "ascii": "rld.www.google.c",
                },
                {
                    "address": 336,
                    "hex": "6f6d 0a31 302e 372e 352e 373a 3339 3833",
                    "ascii": "om.10.7.5.7:3983",
                },
                {"address": 352, "hex": "0a61 7364 666c 6b", "ascii": ".asdflk"},
            ],
            "header": {
                "address": "ADDRESS",
                "hex": "0001 0203 0405 0607 0809 0a0b 0c0d 0e0f",
                "ascii": "ASCII",
            },
            "has_more": False,
            "next_offset": 359,
            "content_length": 359,
        }
        self.assertEqual(expected_result, content)

    @respx.mock
    def test_get_hex_range(self):
        sha256a = "1d4ce380c90339830b7915137b34cd77c627ad498c773f9b4a12a3c876bfe024"
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(side_effect=mock_load_binary)

        # Test taking first 32 bytes of file
        params = {"offset": 0, "shortform": True, "max_bytes_to_read": 32}
        response = self.client.get(f"/v0/binaries/{sha256a}/hexview", params=params)

        self.assertEqual(200, response.status_code)
        content = json.loads(response.content.decode("utf-8"))

        expected_result = {
            "hex_strings": [
                {
                    "address": 0,
                    "hex": "c385 2312 c389 0ac3 aec2 a475 5f3d c394",
                    "ascii": "..#........u_=..",
                },
                {
                    "address": 16,
                    "hex": "c2ac c3a5 c394 c2b8 c384 16c2 b5c3 a8c3",
                    "ascii": "................",
                },
            ],
            "header": {
                "address": "ADDRESS",
                "hex": "0001 0203 0405 0607 0809 0a0b 0c0d 0e0f",
                "ascii": "ASCII",
            },
            "has_more": True,
            "next_offset": 32,
            "content_length": 359,
        }
        self.assertEqual(expected_result, content)

        # Test taking the next 32 bytes
        params = {"offset": 0, "shortform": True, "max_bytes_to_read": 32}
        params["offset"] = content["next_offset"]
        response = self.client.get(f"/v0/binaries/{sha256a}/hexview", params=params)

        self.assertEqual(200, response.status_code)
        content = json.loads(response.content.decode("utf-8"))
        expected_result = {
            "hex_strings": [
                {
                    "address": 32,
                    "hex": "905a c2a1 c280 c3a7 3b45 1a39 c28c c3a3",
                    "ascii": ".Z......;E.9....",
                },
                {
                    "address": 48,
                    "hex": "c281 c284 73c3 9cc2 8b32 1972 32c2 883e",
                    "ascii": "....s....2.r2..>",
                },
            ],
            "header": {
                "address": "ADDRESS",
                "hex": "0001 0203 0405 0607 0809 0a0b 0c0d 0e0f",
                "ascii": "ASCII",
            },
            "has_more": True,
            "next_offset": 64,
            "content_length": 359,
        }
        self.assertEqual(content, expected_result)

        # Test taking the next 32 bytes (in old format)
        params = {"offset": 0, "shortform": False, "max_bytes_to_read": 32}
        params["offset"] = content["next_offset"]
        response = self.client.get(f"/v0/binaries/{sha256a}/hexview", params=params)

        self.assertEqual(200, response.status_code)
        content = json.loads(response.content.decode("utf-8"))

        expected_result = {
            "hex_strings": [
                {
                    "address": 64,
                    "hex": [
                        "7B",
                        "09",
                        "C3",
                        "B2",
                        "C2",
                        "A0",
                        "C3",
                        "A6",
                        "C3",
                        "94",
                        "C2",
                        "80",
                        "C3",
                        "8E",
                        "C3",
                        "A0",
                    ],
                    "ascii": "{...............",
                },
                {
                    "address": 80,
                    "hex": [
                        "44",
                        "C2",
                        "92",
                        "0A",
                        "C3",
                        "8A",
                        "C3",
                        "8C",
                        "C2",
                        "B5",
                        "C3",
                        "BD",
                        "C3",
                        "A8",
                        "3A",
                        "70",
                    ],
                    "ascii": "D.............:p",
                },
            ],
            "header": {
                "address": "ADDRESS",
                "hex": [
                    "00",
                    "01",
                    "02",
                    "03",
                    "04",
                    "05",
                    "06",
                    "07",
                    "08",
                    "09",
                    "0A",
                    "0B",
                    "0C",
                    "0D",
                    "0E",
                    "0F",
                ],
                "ascii": "ASCII",
            },
            "has_more": True,
            "next_offset": 96,
            "content_length": 359,
        }
        self.assertEqual(content, expected_result)

    @respx.mock
    @mock.patch("azul_metastore.query.binary2.binary_read.find_stream_references")
    def test_get_hex_fail(self, cce):
        sha256a = "1d4ce380c90339830b7915137b34cd77c627ad498c773f9b4a12a3c876bfe024"
        respx.get(re.compile(rf"{self.end}/api/v3/stream/source/label/.+")).mock(side_effect=mock_load_binary)

        params = {"offset": 0}
        params["max_bytes_to_read"] = 32
        params["offset"] = 359 + 1

        # Test with out of range
        cce.return_value = (True, "source", "content")
        response = self.client.get(f"/v0/binaries/{sha256a}/hexview", params=params)

        self.assertEqual(500, response.status_code)

        # Test with non existing sha
        cce.return_value = (False, None, None)
        params = {"offset": 0}
        response = self.client.get(f"/v0/binaries/{sha256a[:-1]}f/hexview", params=params)

        self.assertEqual(404, response.status_code)
