import hashlib
from unittest import mock

from tests.support import unit_test


class TestMain(unit_test.DataMockingUnitTest):
    @mock.patch("azul_bedrock.dispatcher.DispatcherAPI", unit_test.FakeDispatcherAPI)
    @mock.patch("azul_metastore.query.binary2.binary_expedite._stream_expeditable")
    def test_post_sha256_expedite(self, mock_event_stream):
        events = [
            {
                "author": {
                    "category": "plugin",
                    "name": "SevenZip",
                    "security": "carrot",
                    "version": "2022.04.01",
                },
                "entity": {
                    "datastreams": [
                        {
                            "label": "content",
                            "md5": "ef090a5c998861a5917bd3e976594f8f",
                            "magic": "PE32 executable (GUI) Intel 80386 " "Mono/.Net assembly, for MS Windows",
                            "mime": "application/x-dosexec",
                            "sha1": "f40825d0089a923ce635ee39c981eefb9fd1f3bb",
                            "sha256": "acae1918dbee5d579b5cdfdd05d9c57f714efa50c2937999f475c569ff4d9cc5",
                            "sha512": "b6dcb9e2bfb285a9201d10e2d9580e4400d31d92e99717eda2a9c29491eb3d3757a8e9764b671d77f4c39b4ee2e3ec0b5effa844ad5eadcac5aec8b78e944d90",
                            "size": 651994,
                        }
                    ],
                    "features": [
                        {
                            "name": "filename",
                            "type": "filepath",
                            "value": "91cc5.exe",
                        },
                        {
                            "name": "magic",
                            "type": "string",
                            "value": "PE32 executable (GUI) Intel 80386 " "Mono/.Net assembly, for MS Windows",
                        },
                        {
                            "name": "mime",
                            "type": "string",
                            "value": "application/x-dosexec",
                        },
                    ],
                    "md5": "ef090a5c998861a5917bd3e976594f8f",
                    "sha1": "f40825d0089a923ce635ee39c981eefb9fd1f3bb",
                    "sha256": "acae1918dbee5d579b5cdfdd05d9c57f714efa50c2937999f475c569ff4d9cc5",
                    "sha512": "b6dcb9e2bfb285a9201d10e2d9580e4400d31d92e99717eda2a9c29491eb3d3757a8e9764b671d77f4c39b4ee2e3ec0b5effa844ad5eadcac5aec8b78e944d90",
                    "size": 651994,
                },
                "action": "extracted",
                "flags": {"expedite": True},
                "source": {
                    "name": "testing",
                    "path": [
                        {
                            "author": {
                                "category": "user",
                                "name": "user",
                                "security": "carrot",
                            },
                            "sha256": "de99ace77d365e7d9c9305d6396a9465004042658a9adcaf0927e3e0d7c2b07c",
                            "filename": "91cc5",
                            "size": 570307,
                            "action": "sourced",
                            "timestamp": "2023-07-07T04:00:00Z",
                        },
                        {
                            "author": {
                                "category": "plugin",
                                "name": "SevenZip",
                                "security": "carrot",
                                "version": "2022.04.01",
                            },
                            "sha256": "acae1918dbee5d579b5cdfdd05d9c57f714efa50c2937999f475c569ff4d9cc5",
                            "filename": "91cc5.exe",
                            "size": 651994,
                            "action": "extracted",
                            "relationship": {"action": "extracted"},
                            "timestamp": "2023-07-07T18:59:16.464812Z",
                        },
                    ],
                    "references": {"user": "user"},
                    "security": "carrot",
                    "timestamp": "2023-07-07T04:00:00Z",
                },
                "timestamp": "2023-07-07T18:59:16.464812Z",
                "model_version": 5,
            }
        ]

        mock_event_stream.return_value = iter(events)
        sha256 = hashlib.sha256(b"hello").hexdigest()
        response = self.client.post(f"/v0/binaries/{sha256}/expedite")
        self.assertEqual(200, response.status_code)
        self.assertEqual(response.headers.get("x-azul-security"), "LOW")

        mock_event_stream.return_value = iter(events)
        sha256 = hashlib.sha256(b"hello").hexdigest()
        response = self.client.post(f"/v0/binaries/{sha256}/expedite", params={"bypass_cache": True})
        self.assertEqual(200, response.status_code)

        mock_event_stream.return_value = iter(events)
        sha256 = hashlib.sha256(b"hello").hexdigest()
        response = self.client.post(f"/v0/binaries/{sha256}/expedite", params={"bypass_cache": False})
        self.assertEqual(200, response.status_code)

        mock_event_stream.return_value = iter(events)
        sha256 = hashlib.sha256(b"hello").hexdigest()
        response = self.client.post(f"/v0/binaries/{sha256}/expedite", params={"bypass_cache": "gerg"})
        self.assertEqual(422, response.status_code)

        mock_event_stream.return_value = iter(events)
        sha256 = hashlib.sha256(b"hello").hexdigest()
        response = self.client.post("/v0/binaries/invalid/expedite")
        self.assertEqual(422, response.status_code)
