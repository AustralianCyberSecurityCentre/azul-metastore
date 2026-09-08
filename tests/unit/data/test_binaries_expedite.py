from azul_metastore.models.basic_events import BinaryEvent
from azul_metastore.query.binary2.binary_expedite import expedite_processing
from unittest import mock
from azul_bedrock import models_network as azm
from azul_bedrock.models_network import SourceSettingsKeys
from tests.support import unit_test
import hashlib


class TestMain(unit_test.DataMockingUnitTest):
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
                            "magic": "PE32 executable (GUI) Intel 80386 Mono/.Net assembly, for MS Windows",
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
                            "value": "PE32 executable (GUI) Intel 80386 Mono/.Net assembly, for MS Windows",
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
                "model_version": azm.CURRENT_MODEL_VERSION,
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

    @mock.patch("azul_metastore.query.binary2.binary_expedite._stream_expeditable")
    def test_post_sha256_plugin_expedite(self, mock_event_stream):
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
                            "magic": "PE32 executable (GUI) Intel 80386 Mono/.Net assembly, for MS Windows",
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
                            "value": "PE32 executable (GUI) Intel 80386 Mono/.Net assembly, for MS Windows",
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
                "model_version": azm.CURRENT_MODEL_VERSION,
            }
        ]
        mock_event_stream.return_value = iter(events)
        sha256 = hashlib.sha256(b"hello").hexdigest()
        response = self.client.post(f"/v0/binaries/{sha256}/expedite?plugin=TestPlugin")
        self.assertEqual(200, response.status_code)
        self.assertEqual(response.headers.get("x-azul-security"), "LOW")

        mock_event_stream.return_value = iter(events)
        sha256 = hashlib.sha256(b"hello").hexdigest()
        response = self.client.post(f"/v0/binaries/{sha256}/expedite?plugin=TestPlugin", params={"bypass_cache": True})
        self.assertEqual(200, response.status_code)

        submission_mock = mock.MagicMock()
        events_sent: list[BinaryEvent] = None

        def accept_processing(*args, **kwargs):
            nonlocal events_sent
            events_sent = args[0]
            return None

        mock_event_stream.return_value = iter(events)
        submission_mock.submit_events.side_effect = accept_processing
        self.ctx.dispatcher = submission_mock
        expedite_processing(
            self.ctx,
            self.ctx,
            "de99ace77d365e7d9c9305d6396a9465004042658a9adcaf0927e3e0d7c2b07c",
            bypass_cache=True,
            plugin="TestPlugin",
        )
        json_events = []
        for evt in events_sent:
            json_events.append(evt.model_dump())
        print(json_events)

        self.assertEqual(events_sent[0].source.settings[SourceSettingsKeys.SETTINGS_DEPTH_REMOVAL_KEY.value], "3")
        self.assertEqual(
            events_sent[0].source.settings[SourceSettingsKeys.SETTINGS_EXPEDITE_PLUGIN_KEY.value], "TestPlugin"
        )

        self.assertEqual(
            json_events,
            [
                {
                    "model_version": 6,
                    "kafka_key": "tmp",
                    "timestamp": "2023-07-07T18:59:16.464812+00:00",
                    "author": {
                        "category": "plugin",
                        "name": "SevenZip",
                        "version": "2022.04.01",
                        "security": "carrot",
                    },
                    "entity": {
                        "sha256": "acae1918dbee5d579b5cdfdd05d9c57f714efa50c2937999f475c569ff4d9cc5",
                        "sha512": "b6dcb9e2bfb285a9201d10e2d9580e4400d31d92e99717eda2a9c29491eb3d3757a8e9764b671d77f4c39b4ee2e3ec0b5effa844ad5eadcac5aec8b78e944d90",
                        "sha1": "f40825d0089a923ce635ee39c981eefb9fd1f3bb",
                        "md5": "ef090a5c998861a5917bd3e976594f8f",
                        "ssdeep": None,
                        "tlsh": None,
                        "size": 651994,
                        "file_format": None,
                        "file_extension": None,
                        "mime": None,
                        "magic": None,
                        "features": [
                            {
                                "name": "filename",
                                "type": azm.FeatureType.Filepath,
                                "value": "91cc5.exe",
                                "label": None,
                                "offset": None,
                                "size": None,
                            },
                            {
                                "name": "magic",
                                "type": azm.FeatureType.String,
                                "value": "PE32 executable (GUI) Intel 80386 Mono/.Net assembly, for MS Windows",
                                "label": None,
                                "offset": None,
                                "size": None,
                            },
                            {
                                "name": "mime",
                                "type": azm.FeatureType.String,
                                "value": "application/x-dosexec",
                                "label": None,
                                "offset": None,
                                "size": None,
                            },
                        ],
                        "datastreams": [
                            {
                                "sha256": "acae1918dbee5d579b5cdfdd05d9c57f714efa50c2937999f475c569ff4d9cc5",
                                "sha512": "b6dcb9e2bfb285a9201d10e2d9580e4400d31d92e99717eda2a9c29491eb3d3757a8e9764b671d77f4c39b4ee2e3ec0b5effa844ad5eadcac5aec8b78e944d90",
                                "sha1": "f40825d0089a923ce635ee39c981eefb9fd1f3bb",
                                "md5": "ef090a5c998861a5917bd3e976594f8f",
                                "ssdeep": None,
                                "tlsh": None,
                                "size": 651994,
                                "file_format": None,
                                "file_extension": None,
                                "mime": "application/x-dosexec",
                                "magic": "PE32 executable (GUI) Intel 80386 Mono/.Net assembly, for MS Windows",
                                "identify_version": 0,
                                "label": "content",
                                "language": None,
                            }
                        ],
                        "info": {},
                    },
                    "action": azm.BinaryAction.Extracted,
                    "source": {
                        "security": "carrot",
                        "name": "testing",
                        "timestamp": "2023-07-07T04:00:00+00:00",
                        "references": {"user": "user"},
                        "path": [
                            {
                                "sha256": "de99ace77d365e7d9c9305d6396a9465004042658a9adcaf0927e3e0d7c2b07c",
                                "action": azm.BinaryAction.Sourced,
                                "timestamp": "2023-07-07T04:00:00+00:00",
                                "author": {"category": "user", "name": "user", "version": None, "security": "carrot"},
                                "relationship": {},
                                "file_format": None,
                                "size": 570307,
                                "filename": "91cc5",
                                "language": None,
                            },
                            {
                                "sha256": "acae1918dbee5d579b5cdfdd05d9c57f714efa50c2937999f475c569ff4d9cc5",
                                "action": azm.BinaryAction.Extracted,
                                "timestamp": "2023-07-07T18:59:16.464812+00:00",
                                "author": {
                                    "category": "plugin",
                                    "name": "SevenZip",
                                    "version": "2022.04.01",
                                    "security": "carrot",
                                },
                                "relationship": {"action": "extracted"},
                                "file_format": None,
                                "size": 651994,
                                "filename": "91cc5.exe",
                                "language": None,
                            },
                        ],
                        "settings": {"remove_at_depth": "3", "expedite_plugin": "TestPlugin"},
                    },
                    "dequeued": None,
                    "retries": None,
                    "flags": {"bypass_cache": True, "expedite": True, "retry": False},
                    "track_source_references": "",
                    "track_links": [],
                    "track_authors": [],
                }
            ],
        )
