import os
from unittest import mock

import httpx
import respx

from tests.support import unit_test
from tests.unit.data.helpers import mock_load_binary_async_iterable_content


class TestMain(unit_test.DataMockingUnitTest):
    # Test AI string filter is not being called if env variable not set and no file_format in URL
    @respx.mock
    @mock.patch.dict(os.environ, {"METASTORE_SMART_STRING_FILTER_URL": ""})
    def test_get_strings_no_filetype_no_env_set(self):
        sha256a = "1d4ce380c90339830b7915137b34cd77c627ad498c773f9b4a12a3c876bfe024"
        # FUTURE use a standard client mock for all these in this file
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(
            side_effect=mock_load_binary_async_iterable_content
        )
        # Mock the POST request
        post_route = respx.post(f"{self.end}/v0/strings?file_format=test").mock(
            return_value=httpx.Response(201, json={"test": "value", "test1": "value"})
        )
        # test retrieve all
        response = self.client.get(f"/v0/binaries/{sha256a}/strings")
        self.assertEqual(200, response.status_code)
        expected_result = {
            "strings": [
                {"string": "Z%d(", "offset": 109, "length": 4, "encoding": "ASCII"},
                {"string": "UTSs", "offset": 130, "length": 4, "encoding": "ASCII"},
                {"string": "'IND", "offset": 266, "length": 4, "encoding": "ASCII"},
                {"string": "helloworld", "offset": 313, "length": 10, "encoding": "ASCII"},
                {"string": "www.google.com", "offset": 324, "length": 14, "encoding": "ASCII"},
                {"string": "10.7.5.7:3983", "offset": 339, "length": 13, "encoding": "ASCII"},
                {"string": "asdflk", "offset": 353, "length": 6, "encoding": "ASCII"},
            ],
            "has_more": False,
            "next_offset": 359,
            "time_out": False,
        }
        self.assertFalse(post_route.called)
        self.assertEqual(expected_result, response.json())

    # Test AI string filter is not being called if env variable not set and file_format in URL
    @respx.mock
    @mock.patch.dict(os.environ, {"METASTORE_SMART_STRING_FILTER_URL": ""})
    def test_get_strings_filetype_no_env_set(self):
        sha256a = "1d4ce380c90339830b7915137b34cd77c627ad498c773f9b4a12a3c876bfe024"
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(
            side_effect=mock_load_binary_async_iterable_content
        )
        # Mock the POST request
        post_route = respx.post(f"{self.end}/v0/strings?file_format=test").mock(
            return_value=httpx.Response(201, json={"test": "value", "test1": "value"})
        )
        # test retrieve all
        response = self.client.get(f"/v0/binaries/{sha256a}/strings?file_format={"test"}")
        self.assertEqual(200, response.status_code)
        expected_result = {
            "strings": [
                {"string": "Z%d(", "offset": 109, "length": 4, "encoding": "ASCII"},
                {"string": "UTSs", "offset": 130, "length": 4, "encoding": "ASCII"},
                {"string": "'IND", "offset": 266, "length": 4, "encoding": "ASCII"},
                {"string": "helloworld", "offset": 313, "length": 10, "encoding": "ASCII"},
                {"string": "www.google.com", "offset": 324, "length": 14, "encoding": "ASCII"},
                {"string": "10.7.5.7:3983", "offset": 339, "length": 13, "encoding": "ASCII"},
                {"string": "asdflk", "offset": 353, "length": 6, "encoding": "ASCII"},
            ],
            "has_more": False,
            "next_offset": 359,
            "time_out": False,
        }
        self.assertFalse(post_route.called)
        self.assertEqual(expected_result, response.json())

    # Test AI string filter is not being called if env variable set and file_format is not in URL
    @respx.mock
    @mock.patch.dict(os.environ, {"METASTORE_SMART_STRING_FILTER_URL": "test"})
    def test_get_strings_no_filetype_env_set(self):
        sha256a = "1d4ce380c90339830b7915137b34cd77c627ad498c773f9b4a12a3c876bfe024"
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(
            side_effect=mock_load_binary_async_iterable_content
        )
        # Mock the POST request
        post_route = respx.post(f"{self.end}/v0/strings?file_format=test").mock(
            return_value=httpx.Response(201, json={"test": "value", "test1": "value"})
        )
        # test retrieve all
        response = self.client.get(f"/v0/binaries/{sha256a}/strings")
        self.assertEqual(200, response.status_code)
        expected_result = {
            "strings": [
                {"string": "Z%d(", "offset": 109, "length": 4, "encoding": "ASCII"},
                {"string": "UTSs", "offset": 130, "length": 4, "encoding": "ASCII"},
                {"string": "'IND", "offset": 266, "length": 4, "encoding": "ASCII"},
                {"string": "helloworld", "offset": 313, "length": 10, "encoding": "ASCII"},
                {"string": "www.google.com", "offset": 324, "length": 14, "encoding": "ASCII"},
                {"string": "10.7.5.7:3983", "offset": 339, "length": 13, "encoding": "ASCII"},
                {"string": "asdflk", "offset": 353, "length": 6, "encoding": "ASCII"},
            ],
            "has_more": False,
            "next_offset": 359,
            "time_out": False,
        }
        self.assertFalse(post_route.called)
        self.assertEqual(expected_result, response.json())

    # Test AI string filter is being called if env variable set and file_format in URL
    @respx.mock
    @mock.patch.dict(os.environ, {"METASTORE_SMART_STRING_FILTER_URL": "http://localhost"})
    def test_get_strings_filetype_env_set(self):
        sha256a = "1d4ce380c90339830b7915137b34cd77c627ad498c773f9b4a12a3c876bfe024"
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(
            side_effect=mock_load_binary_async_iterable_content
        )

        # fake json response
        jr = [
            {"string": "helloworld", "offset": 313},
            {
                "string": "www.google.com",
                "offset": 324,
            },
            {"string": "10.7.5.7:3983", "offset": 339},
        ]

        # Mock the POST request
        post_route = respx.post(f"{self.end}/v0/strings?file_format=test").mock(
            return_value=httpx.Response(201, json=jr)
        )

        response = self.client.get(f"/v0/binaries/{sha256a}/strings?file_format=test")

        self.assertTrue(post_route.called)
        self.assertEqual(200, response.status_code)
        expected_result = {
            "strings": [
                {"string": "helloworld", "offset": 313, "length": 10, "encoding": "ASCII"},
                {
                    "string": "www.google.com",
                    "offset": 324,
                    "length": 14,
                    "encoding": "ASCII",
                },
                {"string": "10.7.5.7:3983", "offset": 339, "length": 13, "encoding": "ASCII"},
            ],
            "has_more": False,
            "next_offset": 0,
            "time_out": False,
        }
        self.assertEqual(expected_result, response.json())

    # Test AI string filter is being called if env variable set and file_format in URL
    @respx.mock
    @mock.patch.dict(os.environ, {"METASTORE_SMART_STRING_FILTER_URL": "http://localhost"})
    def test_get_ai_strings_all_filtered_out(self):
        """Test if the AI filter behaves correctly if no strings are in the file after filtering."""
        sha256a = "1d4ce380c90339830b7915137b34cd77c627ad498c773f9b4a12a3c876bfe024"
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(
            side_effect=mock_load_binary_async_iterable_content
        )

        # fake json response
        jr = []

        # Mock the POST request
        post_route = respx.post(f"{self.end}/v0/strings?filter=not_found_filter_val&file_format=test").mock(
            return_value=httpx.Response(201, json=jr)
        )

        response = self.client.get(f"/v0/binaries/{sha256a}/strings?filter=not_found_filter_val&file_format=test")

        # self.assertTrue(post_route.called)
        self.assertEqual(200, response.status_code)
        expected_result = {"has_more": False, "next_offset": 0, "strings": [], "time_out": False}
        self.assertEqual(expected_result, response.json())

    @respx.mock
    def test_get_strings(self):
        sha256a = "1d4ce380c90339830b7915137b34cd77c627ad498c773f9b4a12a3c876bfe024"
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(
            side_effect=mock_load_binary_async_iterable_content
        )

        # test retrieve all
        response = self.client.get(f"/v0/binaries/{sha256a}/strings")
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 359,
            "strings": [
                {"string": "Z%d(", "offset": 109, "length": 4, "encoding": "ASCII"},
                {"string": "UTSs", "offset": 130, "length": 4, "encoding": "ASCII"},
                {"string": "'IND", "offset": 266, "length": 4, "encoding": "ASCII"},
                {"string": "helloworld", "offset": 313, "length": 10, "encoding": "ASCII"},
                {"string": "www.google.com", "offset": 324, "length": 14, "encoding": "ASCII"},
                {"string": "10.7.5.7:3983", "offset": 339, "length": 13, "encoding": "ASCII"},
                {"string": "asdflk", "offset": 353, "length": 6, "encoding": "ASCII"},
            ],
            "time_out": False,
        }
        print(response.json())
        self.assertEqual(expected_result, response.json())
        self.assertEqual(
            response.headers.get("x-azul-security"), "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"
        )

        # test retrieve all and filter top and bottom sizes.
        response = self.client.get(f"/v0/binaries/{sha256a}/strings?min_length=5&max_length=13")
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 359,
            "strings": [
                {"string": "helloworld", "offset": 313, "length": 10, "encoding": "ASCII"},
                {"string": "10.7.5.7:3983", "offset": 339, "length": 13, "encoding": "ASCII"},
                {"string": "asdflk", "offset": 353, "length": 6, "encoding": "ASCII"},
            ],
            "time_out": False,
        }
        self.assertEqual(expected_result, response.json())
        self.assertEqual(
            response.headers.get("x-azul-security"), "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"
        )

        # test retrieve all case sensitive
        response = self.client.get(f"/v0/binaries/{sha256a.upper()}/strings")
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 359,
            "strings": [
                {"string": "Z%d(", "offset": 109, "length": 4, "encoding": "ASCII"},
                {"string": "UTSs", "offset": 130, "length": 4, "encoding": "ASCII"},
                {"string": "'IND", "offset": 266, "length": 4, "encoding": "ASCII"},
                {"string": "helloworld", "offset": 313, "length": 10, "encoding": "ASCII"},
                {"string": "www.google.com", "offset": 324, "length": 14, "encoding": "ASCII"},
                {"string": "10.7.5.7:3983", "offset": 339, "length": 13, "encoding": "ASCII"},
                {"string": "asdflk", "offset": 353, "length": 6, "encoding": "ASCII"},
            ],
            "time_out": False,
        }
        self.assertEqual(expected_result, response.json())

        # test retrieve 1
        response = self.client.get(f"/v0/binaries/{sha256a}/strings", params={"take_n_strings": 1})
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": True,
            "next_offset": 130,
            "strings": [
                {"string": "Z%d(", "offset": 109, "length": 4, "encoding": "ASCII"},
            ],
            "time_out": False,
        }
        self.assertEqual(expected_result, response.json())

        # test retrieve next 1
        response = self.client.get(
            f"/v0/binaries/{sha256a}/strings",
            params={"take_n_strings": 1, "offset": 130},
        )
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": True,
            "next_offset": 266,
            "strings": [{"string": "UTSs", "offset": 130, "length": 4, "encoding": "ASCII"}],
            "time_out": False,
        }
        self.assertEqual(expected_result, response.json())

        # test retrieve next 1
        response = self.client.get(
            f"/v0/binaries/{sha256a}/strings",
            params={"take_n_strings": 1, "offset": 266},
        )
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": True,
            "next_offset": 313,
            "strings": [{"string": "'IND", "offset": 266, "length": 4, "encoding": "ASCII"}],
            "time_out": False,
        }
        self.assertEqual(expected_result, response.json())

    @respx.mock
    def test_get_strings_utf16_little_endian(self):
        sha256a = "0ca0bafc3d3fd6960c7f7bc6c63064279746bab14ea7aaed735f43049c9627dc"
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(
            side_effect=mock_load_binary_async_iterable_content
        )

        # test retrieve all
        response = self.client.get(f"/v0/binaries/{sha256a}/strings")
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 224,
            "strings": [
                {
                    "string": "sfa3fiojvas90aw3lkafse9sdalkj;",
                    "offset": 2,
                    "length": 60,
                    "encoding": "UTF-16",
                },
                {
                    "string": "afseo9[pawr3lkj;fse09 klmas",
                    "offset": 64,
                    "length": 54,
                    "encoding": "UTF-16",
                },
                {"string": "fsda09fdsa;'l", "offset": 120, "length": 26, "encoding": "UTF-16"},
                {"string": "vds90esa", "offset": 148, "length": 16, "encoding": "UTF-16"},
                {"string": "vds9evsa", "offset": 166, "length": 16, "encoding": "UTF-16"},
                {"string": "vdsa9evsal", "offset": 184, "length": 20, "encoding": "UTF-16"},
                {"string": "svad9vsda", "offset": 206, "length": 18, "encoding": "UTF-16"},
            ],
            "time_out": False,
        }
        self.assertEqual(expected_result, response.json())

        # test retrieve but filter out large strings (filters out strings larger than 26 characters when decoded.)
        response = self.client.get(f"/v0/binaries/{sha256a}/strings?min_length=8&max_length=20")
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 224,
            "strings": [
                {"string": "fsda09fdsa;'l", "offset": 120, "length": 26, "encoding": "UTF-16"},
                {"string": "vds90esa", "offset": 148, "length": 16, "encoding": "UTF-16"},
                {"string": "vds9evsa", "offset": 166, "length": 16, "encoding": "UTF-16"},
                {"string": "vdsa9evsal", "offset": 184, "length": 20, "encoding": "UTF-16"},
                {"string": "svad9vsda", "offset": 206, "length": 18, "encoding": "UTF-16"},
            ],
            "time_out": False,
        }
        self.assertEqual(expected_result, response.json())

        # test retrieve but filter out large strings (filters out strings larger than 26 characters and longer than 9 characters.)
        response = self.client.get(f"/v0/binaries/{sha256a}/strings?min_length=9&max_length=20")
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 224,
            "strings": [
                {"string": "fsda09fdsa;'l", "offset": 120, "length": 26, "encoding": "UTF-16"},
                {"string": "vdsa9evsal", "offset": 184, "length": 20, "encoding": "UTF-16"},
                {"string": "svad9vsda", "offset": 206, "length": 18, "encoding": "UTF-16"},
            ],
            "time_out": False,
        }
        self.assertEqual(expected_result, response.json())

    @respx.mock
    def test_get_filtered_strings(self):
        sha256a = "1d4ce380c90339830b7915137b34cd77c627ad498c773f9b4a12a3c876bfe024"
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(
            side_effect=mock_load_binary_async_iterable_content
        )

        # test retrieve all, filtering for case-insensitive google string
        response = self.client.get(f"/v0/binaries/{sha256a}/strings", params={"filter": "GooGlE"})
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 359,
            "strings": [
                {"string": "www.google.com", "offset": 324, "length": 14, "encoding": "ASCII"},
            ],
            "time_out": False,
        }
        print(response.json())
        self.assertEqual(expected_result, response.json())

        # test retrieve all, no results returned for unrelated string
        response = self.client.get(f"/v0/binaries/{sha256a}/strings", params={"filter": "bing"})
        self.assertEqual(200, response.status_code)
        expected_result = {"has_more": False, "next_offset": 359, "strings": [], "time_out": False}
        self.assertEqual(expected_result, response.json())

        # test retrieve 1, filtering for a string that *isn't* the first string in the file
        response = self.client.get(f"/v0/binaries/{sha256a}/strings", params={"filter": ":3983", "take_n_strings": 1})
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 359,
            "strings": [
                {"string": "10.7.5.7:3983", "offset": 339, "length": 13, "encoding": "ASCII"},
            ],
            "time_out": False,
        }
        self.assertEqual(expected_result, response.json())

    @respx.mock
    def test_get_regex_strings(self):
        sha256a = "1d4ce380c90339830b7915137b34cd77c627ad498c773f9b4a12a3c876bfe024"
        respx.get(f"{self.end}/api/v3/stream/source/label/{sha256a}").mock(
            side_effect=mock_load_binary_async_iterable_content
        )

        # test retrieve all, filtering for case-insensitive google string
        response = self.client.get(f"/v0/binaries/{sha256a}/strings", params={"regex": "^www."})
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 359,
            "strings": [
                {"string": "www.google.com", "offset": 324, "length": 14, "encoding": "ASCII"},
            ],
            "time_out": False,
        }
        self.assertEqual(expected_result, response.json())

        # test retrieve all, no results returned for string which does not finish on EOL
        response = self.client.get(f"/v0/binaries/{sha256a}/strings", params={"regex": "asd$"})
        self.assertEqual(200, response.status_code)
        expected_result = {"has_more": False, "next_offset": 359, "strings": [], "time_out": False}
        self.assertEqual(expected_result, response.json())

        # test retrieve 1, filtering for a string that *isn't* the first string in the file
        response = self.client.get(
            f"/v0/binaries/{sha256a}/strings", params={"regex": ":[0-9]{4}", "take_n_strings": 1}
        )
        self.assertEqual(200, response.status_code)
        expected_result = {
            "has_more": False,
            "next_offset": 359,
            "strings": [
                {"string": "10.7.5.7:3983", "offset": 339, "length": 13, "encoding": "ASCII"},
            ],
            "time_out": False,
        }
        self.assertEqual(expected_result, response.json())

        # test for a bad regex
        response = self.client.get(f"/v0/binaries/{sha256a}/strings", params={"regex": "["})
        self.assertEqual(400, response.status_code)
        self.assertEqual("Invalid regex pattern", response.json()["detail"]["ref"])
