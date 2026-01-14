from azul_bedrock import models_network as azm

from azul_metastore.query.binary2 import binary_submit_manual
from tests.support import gen, integration_test


class TestBasic(integration_test.DynamicTestCase):

    def test_manual_insert(self):
        event = gen.manual_insert()
        ev2 = list(binary_submit_manual._stream_append_manual_insert(self.writer, event, {}))
        self.assertEqual(0, len(ev2))

        self.write_binary_events(
            [
                gen.binary_event(eid="e1"),
                gen.binary_event(
                    eid="e1", authornv=("a1", "1"), timestamp="2024-01-01T01:01:01Z"
                ),  # should be indexed in preference to everything else as it's timestamp is the newest.
                gen.binary_event(eid="e1", sourceit=("s3", "2000-01-01T01:01:01Z")),
                gen.binary_event(eid="e2"),
            ]
        )
        ev2 = list(binary_submit_manual._stream_append_manual_insert(self.writer, event, {}))
        ev2.sort(key=lambda x: x.source.name)
        self.assertEqual(len(ev2), 2)
        self.assertFormatted(
            ev2[0].model_dump(mode="json", exclude_defaults=True),
            {
                "model_version": azm.CURRENT_MODEL_VERSION,
                "kafka_key": "meta-tmp",
                "timestamp": "2021-03-30T21:44:50.703063+00:00",
                "author": {"category": "user", "name": "user1", "security": "LOW TLP:CLEAR"},
                "entity": {
                    "sha256": "e1111",
                    "sha512": "abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab",
                    "sha1": "abababababababababababababababababababab",
                    "md5": "abababababababababababababababab",
                    "size": 121,
                    "features": [
                        {"name": "magic", "type": "string", "value": "ASCII text"},
                        {"name": "mime", "type": "string", "value": "text/plain"},
                    ],
                    "datastreams": [
                        {
                            "sha256": "00000000000000000000000000000000000000000000000000000000000000ab",
                            "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ab",
                            "sha1": "00000000000000000000000000000000000000ab",
                            "md5": "000000000000000000000000000000ab",
                            "ssdeep": "1:1:1",
                            "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                            "size": 1024,
                            "file_format": "text/plain",
                            "file_extension": "txt",
                            "mime": "text/plain",
                            "magic": "ASCII text",
                            "identify_version": 1,
                            "label": "content",
                        }
                    ],
                },
                "action": "sourced",
                "source": {
                    "name": "generic_source",
                    "path": [
                        {
                            "sha256": "e1",
                            "action": "sourced",
                            "timestamp": "2024-01-01T01:01:01+00:00",
                            "author": {
                                "category": "plugin",
                                "name": "a1",
                                "version": "1",
                                "security": "LOW TLP:CLEAR",
                            },
                            "file_format": "text/plain",
                            "size": 1024,
                        },
                        {
                            "sha256": "e1111",
                            "action": "sourced",
                            "timestamp": "2020-06-02T11:47:03.200000+00:00",
                            "author": {"category": "user", "name": "user1", "security": "LOW TLP:CLEAR"},
                            "size": 121,
                            "filename": "test.txt",
                        },
                    ],
                    "timestamp": "2021-01-01T11:00:00+00:00",
                    "security": "LOW TLP:CLEAR",
                    "references": {"ref1": "val1", "ref2": "val2"},
                },
                "track_source_references": "generic_source.dd6e233ae7a843de99f9b43c349069e4",
                "track_links": ["e1.e1111.user.user1.None"],
                "track_authors": ["plugin.a1.1", "user.user1.None"],
            },
        )
        self.assertFormatted(
            ev2[1].model_dump(mode="json", exclude_defaults=True),
            {
                "model_version": azm.CURRENT_MODEL_VERSION,
                "kafka_key": "meta-tmp",
                "timestamp": "2021-03-30T21:44:50.703063+00:00",
                "author": {"category": "user", "name": "user1", "security": "LOW TLP:CLEAR"},
                "entity": {
                    "sha256": "e1111",
                    "sha512": "abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab",
                    "sha1": "abababababababababababababababababababab",
                    "md5": "abababababababababababababababab",
                    "size": 121,
                    "features": [
                        {"name": "magic", "type": "string", "value": "ASCII text"},
                        {"name": "mime", "type": "string", "value": "text/plain"},
                    ],
                    "datastreams": [
                        {
                            "sha256": "00000000000000000000000000000000000000000000000000000000000000ab",
                            "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ab",
                            "sha1": "00000000000000000000000000000000000000ab",
                            "md5": "000000000000000000000000000000ab",
                            "ssdeep": "1:1:1",
                            "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                            "size": 1024,
                            "file_format": "text/plain",
                            "file_extension": "txt",
                            "mime": "text/plain",
                            "magic": "ASCII text",
                            "identify_version": 1,
                            "label": "content",
                        }
                    ],
                },
                "action": "sourced",
                "source": {
                    "name": "s3",
                    "path": [
                        {
                            "sha256": "e1",
                            "action": "sourced",
                            "timestamp": "2021-01-01T12:00:00+00:00",
                            "author": {
                                "category": "plugin",
                                "name": "generic_plugin",
                                "version": "2021-01-01T12:00:00+00:00",
                                "security": "LOW TLP:CLEAR",
                            },
                            "file_format": "text/plain",
                            "size": 1024,
                        },
                        {
                            "sha256": "e1111",
                            "action": "sourced",
                            "timestamp": "2020-06-02T11:47:03.200000+00:00",
                            "author": {"category": "user", "name": "user1", "security": "LOW TLP:CLEAR"},
                            "size": 121,
                            "filename": "test.txt",
                        },
                    ],
                    "timestamp": "2000-01-01T01:01:01+00:00",
                    "security": "LOW TLP:CLEAR",
                    "references": {"ref1": "val1", "ref2": "val2"},
                },
                "track_source_references": "s3.dd6e233ae7a843de99f9b43c349069e4",
                "track_links": ["e1.e1111.user.user1.None"],
                "track_authors": ["plugin.generic_plugin.2021-01-01T12:00:00+00:00", "user.user1.None"],
            },
        )
