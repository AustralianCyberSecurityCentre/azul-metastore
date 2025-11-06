from azul_metastore.models import basic_events
from tests.support import gen, unit_test


class TestPluginEvent(unit_test.BaseUnitTestCase):
    def test_standard(self):
        d = basic_events.PluginEvent.normalise(gen.plugin())
        self.assertFormatted(
            d,
            {
                "kafka_key": "meta.generic_plugin.2021-01-01T12:00:00+00:00",
                "model_version": 5,
                "timestamp": "2000-01-01T01:01:01+00:00",
                "author": {
                    "category": "plugin",
                    "name": "generic_plugin",
                    "version": "2021-01-01T12:00:00+00:00",
                    "security": "LOW TLP:CLEAR",
                },
                "entity": {
                    "category": "plugin",
                    "name": "generic_plugin",
                    "version": "2021-01-01T12:00:00+00:00",
                    "security": "LOW TLP:CLEAR",
                    "description": "generic_description",
                    "contact": "generic_contact",
                    "features": [{"name": "generic_feature", "desc": "generic_description", "type": "string"}],
                },
            },
        )


class TestStatusEvent(unit_test.BaseUnitTestCase):
    def test_standard(self):
        d = basic_events.StatusEvent.normalise(gen.status())
        self.assertFormatted(
            d,
            {
                "model_version": 5,
                "kafka_key": "meta.meta.4786f82201a89be6c083ce9b06ec41e3.generic_plugin.1.generic_plugin",
                "timestamp": "2000-01-01T01:01:01+00:00",
                "author": {
                    "category": "plugin",
                    "name": "generic_plugin",
                    "version": "1",
                    "security": "LOW TLP:CLEAR",
                },
                "entity": {
                    "input": {
                        "model_version": 5,
                        "kafka_key": "meta.4786f82201a89be6c083ce9b06ec41e3",
                        "timestamp": "2021-01-01T12:00:00+00:00",
                        "author": {
                            "category": "plugin",
                            "name": "generic_plugin",
                            "version": "1",
                            "security": "LOW TLP:CLEAR",
                        },
                        "action": "sourced",
                        "source": {
                            "name": "generic_source",
                            "path": [
                                {
                                    "sha256": "test-meta-tmp",
                                    "action": "sourced",
                                    "timestamp": "2021-01-01T12:00:00+00:00",
                                    "author": {
                                        "category": "plugin",
                                        "name": "generic_plugin",
                                        "version": "1",
                                        "security": "LOW TLP:CLEAR",
                                    },
                                    "relationship": {"random": "data", "action": "extracted", "label": "within"},
                                    "file_format_legacy": "Text",
                                    "file_format": "text/plain",
                                    "size": 1024,
                                }
                            ],
                            "timestamp": "2000-01-01T01:01:01+00:00",
                            "security": "LOW TLP:CLEAR",
                            "references": {"ref1": "val1", "ref2": "val2"},
                        },
                        "entity": {
                            "sha256": "test-meta-tmp",
                            "sha512": "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000test-meta-tmp",
                            "sha1": "000000000000000000000000000test-meta-tmp",
                            "md5": "0000000000000000000test-meta-tmp",
                            "ssdeep": "1:1:1",
                            "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                            "size": 1024,
                            "file_format_legacy": "Text",
                            "file_format": "text/plain",
                            "file_extension": "txt",
                            "mime": "text/plain",
                            "magic": "ASCII text",
                            "datastreams": [
                                {
                                    "sha256": "test-meta-tmp",
                                    "sha512": "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000test-meta-tmp",
                                    "sha1": "000000000000000000000000000test-meta-tmp",
                                    "md5": "0000000000000000000test-meta-tmp",
                                    "ssdeep": "1:1:1",
                                    "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                                    "size": 1024,
                                    "file_format_legacy": "Text",
                                    "file_format": "text/plain",
                                    "file_extension": "txt",
                                    "mime": "text/plain",
                                    "magic": "ASCII text",
                                    "identify_version": 1,
                                    "label": "content",
                                }
                            ],
                            "features": [{"name": "generic_feature", "type": "string", "value": "generic_value"}],
                        },
                        "dequeued": "meta.4786f82201a89be6c083ce9b06ec41e3.generic_plugin.1",
                        "track_source_references": "generic_source.dd6e233ae7a843de99f9b43c349069e4",
                        "track_authors": ["plugin.generic_plugin.1"],
                    },
                    "status": "heartbeat",
                    "runtime": 10.0,
                },
            },
        )


class TestBinaryEvent(unit_test.BaseUnitTestCase):
    def test_standard(self):
        d = basic_events.BinaryEvent.normalise(gen.binary_event())
        self.assertFormatted(
            d,
            {
                "kafka_key": "meta.2248b4a16786fbc2585b83a4ef4e941e",
                "action": "sourced",
                "model_version": 5,
                "timestamp": "2021-01-01T12:00:00+00:00",
                "source": {
                    "name": "generic_source",
                    "path": [
                        {
                            "action": "sourced",
                            "timestamp": "2021-01-01T12:00:00+00:00",
                            "author": {
                                "category": "plugin",
                                "name": "generic_plugin",
                                "version": "2021-01-01T12:00:00+00:00",
                                "security": "LOW TLP:CLEAR",
                            },
                            "sha256": "00000000000000000000000000000000000000000000000000000000000000ab",
                            "file_format_legacy": "Text",
                            "file_format": "text/plain",
                            "size": 1024,
                            "relationship": {"random": "data", "action": "extracted", "label": "within"},
                        }
                    ],
                    "timestamp": "2021-01-01T11:00:00+00:00",
                    "security": "LOW TLP:CLEAR",
                    "references": {"ref1": "val1", "ref2": "val2"},
                },
                "author": {
                    "category": "plugin",
                    "name": "generic_plugin",
                    "version": "2021-01-01T12:00:00+00:00",
                    "security": "LOW TLP:CLEAR",
                },
                "entity": {
                    "md5": "000000000000000000000000000000ab",
                    "sha1": "00000000000000000000000000000000000000ab",
                    "sha256": "00000000000000000000000000000000000000000000000000000000000000ab",
                    "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ab",
                    "ssdeep": "1:1:1",
                    "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                    "size": 1024,
                    "file_format_legacy": "Text",
                    "file_format": "text/plain",
                    "file_extension": "txt",
                    "mime": "text/plain",
                    "magic": "ASCII text",
                    "features": [{"name": "generic_feature", "type": "string", "value": "generic_value"}],
                    "datastreams": [
                        {
                            "md5": "000000000000000000000000000000ab",
                            "sha1": "00000000000000000000000000000000000000ab",
                            "sha256": "00000000000000000000000000000000000000000000000000000000000000ab",
                            "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ab",
                            "ssdeep": "1:1:1",
                            "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                            "size": 1024,
                            "file_format_legacy": "Text",
                            "file_format": "text/plain",
                            "file_extension": "txt",
                            "mime": "text/plain",
                            "magic": "ASCII text",
                            "identify_version": 1,
                            "label": "content",
                        }
                    ],
                },
                "dequeued": "a dequeued id",
                "retries": 0,
                "track_source_references": "generic_source.dd6e233ae7a843de99f9b43c349069e4",
                "track_authors": ["plugin.generic_plugin.2021-01-01T12:00:00+00:00"],
            },
        )
