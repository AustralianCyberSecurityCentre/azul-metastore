import importlib

import respx
from azul_bedrock.models_restapi import binaries_data as bedr_binaries_data
from tests.support import unit_test
from tests.unit.data.helpers import mock_load_binary_async_iterable_content
from azul_metastore.common import data_common, data_strings


class TestCommonStrings(unit_test.DataMockingUnitTest):
    def setUp(self) -> None:
        self.sha256a = "0000000000000000000000000000000000000000000000000000000000000001"
        self.sha256b = "0000000000000000000000000000000000000000000000000000000000000002"
        return super().setUp()

    def setup_mocks(self):
        """Setup the basic respx mocking."""
        respx.get(f"{self.end}/api/v3/stream/source/label/{self.sha256a}").mock(
            side_effect=mock_load_binary_async_iterable_content
        )
        respx.get(f"{self.end}/api/v3/stream/source/label/{self.sha256b}").mock(
            side_effect=mock_load_binary_async_iterable_content
        )

    @respx.mock
    def test_get_all_strings(self):
        self.setup_mocks()
        # test retrieve all
        response = self.client.get(f"/v0/binaries/{self.sha256a}/{self.sha256b}/strings")
        self.assertEqual(200, response.status_code)
        print(response.json())
        result = bedr_binaries_data.CommonBinaryStrings.model_validate(response.json())
        expected_result = bedr_binaries_data.CommonBinaryStrings(
            incomplete=False,
            strings=[
                "# Randomly generated content for the purpose of strings compare testing",
                "This",
                "access",
                "connected",
                "created",
                "decay.",
                "drilled",
                "drives",
                "formation",
                "generators.",
                "gradients",
                "naturally",
                "occurring",
                "planet",
                "power",
                "radioactive",
                "reduce",
                "steam,",
                "systems,",
                "temperature",
                "that",
                "turbines",
                "water",
                "wells",
                "which",
            ],
        )
        self.assertEqual(result, expected_result)
        self.assertEqual(
            response.headers.get("x-azul-security"), "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"
        )

    @respx.mock
    def test_get_strings_upper_and_lower_lengths(self):
        """Read all the strings but filter them based on string length to be less than the default all."""
        self.setup_mocks()
        # test retrieve all
        response = self.client.get(f"/v0/binaries/{self.sha256a}/{self.sha256b}/strings?min_length=7&max_length=15")
        self.assertEqual(200, response.status_code)
        print(response.json())
        result = bedr_binaries_data.CommonBinaryStrings.model_validate(response.json())
        expected_result = bedr_binaries_data.CommonBinaryStrings(
            incomplete=False,
            strings=[
                "connected",
                "created",
                "drilled",
                "formation",
                "generators.",
                "gradients",
                "naturally",
                "occurring",
                "radioactive",
                "systems,",
                "temperature",
                "turbines",
            ],
        )
        self.assertEqual(result, expected_result)
        self.assertEqual(
            response.headers.get("x-azul-security"), "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"
        )

    @respx.mock
    def test_get_strings_part_of_files(self):
        """Get strings but limit the total amount of bytes read to under the 1kb file sizes."""
        self.setup_mocks()
        # test retrieve all
        response = self.client.get(f"/v0/binaries/{self.sha256a}/{self.sha256b}/strings?max_bytes_to_read=500")
        self.assertEqual(200, response.status_code)
        print(response.json())
        result = bedr_binaries_data.CommonBinaryStrings.model_validate(response.json())
        expected_result = bedr_binaries_data.CommonBinaryStrings(
            incomplete=True,
            strings=[
                "# Randomly generated content for the purpose of strings compare testing",
                "created",
                "decay.",
                "drilled",
                "formation",
                "gradients",
                "naturally",
                "occurring",
                "planet",
                "power",
                "radioactive",
                "systems,",
                "temperature",
                "wells",
            ],
        )
        self.assertEqual(result, expected_result)
        self.assertEqual(
            response.headers.get("x-azul-security"), "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"
        )

    @respx.mock
    def test_get_strings_limit_strings(self):
        """Get the strings but use query parameters to limit the total number of strings returned to less than all.

        Note - all strings are still acquired due to the way data_strings buffers content and the total length of the file in the inital test case.
        """
        self.setup_mocks()
        # test retrieve all
        response = self.client.get(f"/v0/binaries/{self.sha256a}/{self.sha256b}/strings?take_n_strings=5")
        self.assertEqual(200, response.status_code)
        print(response.json())
        result = bedr_binaries_data.CommonBinaryStrings.model_validate(response.json())
        expected_result = bedr_binaries_data.CommonBinaryStrings(
            incomplete=False,
            strings=[
                "# Randomly generated content for the purpose of strings compare testing",
                "This",
                "access",
                "connected",
                "created",
                "decay.",
                "drilled",
                "drives",
                "formation",
                "generators.",
                "gradients",
                "naturally",
                "occurring",
                "planet",
                "power",
                "radioactive",
                "reduce",
                "steam,",
                "systems,",
                "temperature",
                "that",
                "turbines",
                "water",
                "wells",
                "which",
            ],
        )
        self.assertEqual(result, expected_result)
        self.assertEqual(
            response.headers.get("x-azul-security"), "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"
        )

        ### Another run with the buffer cut down.
        # test retrieve less after chunk size reduced. (have to reload to load the new buffer_size into binaries_data)
        original_size = data_common.DEFAULT_STRING_BUFFER_SIZE
        data_common.DEFAULT_STRING_BUFFER_SIZE = 100
        importlib.reload(data_strings)

        def cleanup():
            data_common.DEFAULT_STRING_BUFFER_SIZE = original_size
            importlib.reload(data_strings)

        self.addCleanup(cleanup)

        response = self.client.get(f"/v0/binaries/{self.sha256a}/{self.sha256b}/strings?take_n_strings=5")
        self.assertEqual(200, response.status_code)
        print(response.json())
        result = bedr_binaries_data.CommonBinaryStrings.model_validate(response.json())
        expected_result = bedr_binaries_data.CommonBinaryStrings(
            incomplete=True,
            strings=[
                "# Randomly generated content for the purpose of strings compare testing",
                "created",
                "decay.",
                "drilled",
                "formation",
                "gradients",
                "naturally",
                "occurring",
                "planet",
                "power",
                "radioactive",
                "systems,",
                "temperature",
                "wells",
            ],
        )
        self.assertEqual(result, expected_result)
        self.assertEqual(
            response.headers.get("x-azul-security"), "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"
        )
