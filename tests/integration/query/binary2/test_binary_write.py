from azul_bedrock import models_network as azm

from azul_metastore.query import binary_create
from tests.support import gen, integration_test


class TestSearchEntity(integration_test.DynamicTestCase):

    def test_legacy_relationship_none(self):
        # test edge case from older things
        tmp = gen.binary_event(eid="e1", authornv=("a1", "1"))
        tmp.source.path[0].relationship = None
        self.write_binary_events([tmp])
        results = self.read_binary_events("e1")
        self.assertEqual(1, len(results))

    def test_create_duplicate_events(self):
        errors, duplicate_count = binary_create.create_binary_events(
            self.system.writer,
            [
                gen.binary_event(
                    eid="e1",
                    authornv=("a1", "1"),
                ),
                gen.binary_event(
                    eid="e1",
                    authornv=("a1", "1"),
                ),
                gen.binary_event(
                    eid="e1",
                    authornv=("a1", "1"),
                ),
                gen.binary_event(
                    eid="e1",
                    authornv=("a1", "1"),
                ),
            ],
        )
        self.assertEqual(duplicate_count, 3)

    def test_mapped(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    authornv=("a1", "1"),
                    action="mapped",
                    spathl=[],
                ),
                gen.binary_event(
                    eid="e10",
                    authornv=("a1", "1"),
                    action="mapped",
                    spathl=[("e1", ("p1", "1"))],
                ),
            ],
        )
        ev = [x for x in self.read_binary_events("e1", raw=True) if "sha256" in x][0]
        ev.pop("_index")
        ev.pop("_id")
        self.assertFormatted(
            ev,
            {
                "binary_info": {"name": "metadata", "parent": "e1"},
                "security": "LOW TLP:CLEAR",
                "encoded_security": {
                    "exclusive": ["s-low"],
                    "inclusive": ["s-any"],
                    "markings": ["s-tlp-clear"],
                    "num_exclusive": 1,
                },
                "timestamp": "2021-01-01T12:00:00Z",
                "action": "mapped",
                "author": {"category": "plugin", "name": "a1", "version": "1", "security": "LOW TLP:CLEAR"},
                "track_author": "plugin.a1.1",
                "sha256": "e1",
                "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e1",
                "sha1": "00000000000000000000000000000000000000e1",
                "md5": "000000000000000000000000000000e1",
                "ssdeep": "1:1:1",
                "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                "size": 1024,
                "file_format": "text/plain",
                "file_extension": "txt",
                "mime": "text/plain",
                "magic": "ASCII text",
                "features": [
                    {
                        "name": "generic_feature",
                        "type": "string",
                        "value": "generic_value",
                        "enriched": {},
                        "encoded": {},
                    }
                ],
                "encoded_ssdeep": {"blocksize": 1, "chunk": "1", "dchunk": "1"},
                "tlsh_vector": [
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                ],
                "features_map": {"generic_feature": ["generic_value"]},
                "sha256_author_action": "e1.plugin.a1.mapped",
                "uniq_features": "e1.plugin.a1.mapped.5215d5851701bb31fb24e7ecf9e1048f",
                "num_feature_names": 1,
                "num_feature_values": 1,
                "depth": 0,
                "source": {
                    "name": "generic_source",
                    "timestamp": "2021-01-01T11:00:00Z",
                    "security": "LOW TLP:CLEAR",
                    "references": {"ref1": "val1", "ref2": "val2"},
                    "encoded_references": [
                        {"key": "ref1", "value": "val1", "key_value": "ref1.val1"},
                        {"key": "ref2", "value": "val2", "key_value": "ref2.val2"},
                    ],
                },
                "track_source_references": "generic_source.dd6e233ae7a843de99f9b43c349069e4",
            },
        )
        ev = [x for x in self.read_binary_events("e10", raw=True) if "sha256" in x][0]
        ev.pop("_index")
        ev.pop("_id")
        self.assertFormatted(
            ev,
            {
                "binary_info": {"name": "metadata", "parent": "e10"},
                "security": "LOW TLP:CLEAR",
                "encoded_security": {
                    "exclusive": ["s-low"],
                    "inclusive": ["s-any"],
                    "markings": ["s-tlp-clear"],
                    "num_exclusive": 1,
                },
                "timestamp": "2021-01-01T12:00:00Z",
                "action": "mapped",
                "author": {"category": "plugin", "name": "a1", "version": "1", "security": "LOW TLP:CLEAR"},
                "track_author": "plugin.a1.1",
                "sha256": "e10",
                "sha512": "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e10",
                "sha1": "0000000000000000000000000000000000000e10",
                "md5": "00000000000000000000000000000e10",
                "ssdeep": "1:1:1",
                "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                "size": 1024,
                "file_format": "text/plain",
                "file_extension": "txt",
                "mime": "text/plain",
                "magic": "ASCII text",
                "features": [
                    {
                        "name": "generic_feature",
                        "type": "string",
                        "value": "generic_value",
                        "enriched": {},
                        "encoded": {},
                    }
                ],
                "encoded_ssdeep": {"blocksize": 1, "chunk": "1", "dchunk": "1"},
                "tlsh_vector": [
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                    -128,
                ],
                "features_map": {"generic_feature": ["generic_value"]},
                "sha256_author_action": "e10.plugin.a1.mapped",
                "uniq_features": "e10.plugin.a1.mapped.5215d5851701bb31fb24e7ecf9e1048f",
                "num_feature_names": 1,
                "num_feature_values": 1,
                "depth": 1,
                "source": {
                    "name": "generic_source",
                    "timestamp": "2021-01-01T11:00:00Z",
                    "security": "LOW TLP:CLEAR",
                    "references": {"ref1": "val1", "ref2": "val2"},
                    "encoded_references": [
                        {"key": "ref1", "value": "val1", "key_value": "ref1.val1"},
                        {"key": "ref2", "value": "val2", "key_value": "ref2.val2"},
                    ],
                },
                "track_source_references": "generic_source.dd6e233ae7a843de99f9b43c349069e4",
                "track_link": "e1.e10.plugin.a1.1",
                "parent": {
                    "sha256": "e1",
                    "action": "sourced",
                    "timestamp": "2021-01-01T12:00:00Z",
                    "author": {"category": "plugin", "name": "p1", "version": "1", "security": "LOW TLP:CLEAR"},
                    "relationship": {"random": "data", "action": "extracted", "label": "within"},
                    "file_format": "text/plain",
                    "size": 1024,
                    "encoded": {"sha256_author_action": "e1.plugin.p1.sourced"},
                },
                "parent_track_author": "plugin.p1.1",
                "parent_relationship": {"random": "data", "action": "extracted", "label": "within"},
            },
        )

    def test_too_many_streams(self):
        self.write_binary_events([])
        res = gen.binary_event(datas=[gen.data({"label": azm.DataLabel.TEST}) for x in range(150)])
        errors, duplicates = binary_create.create_binary_events(self.writer, [res])
        self.assertEqual(1, errors)
        self.flush()
        results = self.read_binary_events("generic_binary")
        self.assertEqual(0, len(results))

    def test_bad_source(self):
        self.write_binary_events([gen.binary_event(sourceit=("invalid1", "2000-01-01T00:00:00Z"))], must_error=1)
        self.assertEqual(0, self.count_binary_events("generic_binary"))

    def test_casing(self):
        self.write_binary_events([gen.binary_event(eid="CASING", authornv=("a1", "1"))])
        self.assertEqual(2, self.count_binary_events("casing"))
        self.assertEqual(2, self.count_binary_events("CASING"))
        results = self.read_binary_events("casing")
        self.assertEqual(f"{'casing':0>32}", results[0]["entity"]["md5"])
        self.assertEqual(f"{'casing':0>40}", results[0]["entity"]["sha1"])
        self.assertEqual("casing", results[0]["entity"]["sha256"])
        self.assertEqual(f"{'casing':0>128}", results[0]["entity"]["sha512"])

    def test_keeps_different_authors(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="generic_binary", authornv=("a1", "1")),
                gen.binary_event(eid="generic_binary", authornv=("a2", "1")),
            ]
        )
        self.assertEqual(3, self.count_binary_events("generic_binary"))

    def test_keeps_different_entities(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1"),
                gen.binary_event(eid="e2"),
            ]
        )
        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e2"))

    def test_extended_properties(self):
        be = gen.binary_event(eid="e1")

        self.write_binary_events([be])
        results = self.read_binary_events("e1")
        print(results[0]["entity"])
        self.assertEqual(len(results), 1)
        self.assertEqual(
            results[0]["entity"],
            {
                "size": 1024,
                "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e1",
                "sha256": "e1",
                "sha1": "00000000000000000000000000000000000000e1",
                "md5": "000000000000000000000000000000e1",
                "ssdeep": "1:1:1",
                "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                "mime": "text/plain",
                "magic": "ASCII text",
                "file_format": "text/plain",
                "file_extension": "txt",
                "features": [
                    {
                        "name": "generic_feature",
                        "type": "string",
                        "value": "generic_value",
                    }
                ],
                "datastreams": [
                    {
                        "identify_version": 1,
                        "label": "content",
                        "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e1",
                        "sha256": "e1",
                        "sha1": "00000000000000000000000000000000000000e1",
                        "md5": "000000000000000000000000000000e1",
                        "size": 1024,
                        "ssdeep": "1:1:1",
                        "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                        "mime": "text/plain",
                        "magic": "ASCII text",
                        "file_format": "text/plain",
                        "file_extension": "txt",
                    }
                ],
            },
        )

    def test_no_relationship(self):
        ev = gen.binary_event(
            eid="e10",
            authornv=("a1", "1"),
            action="mapped",
            spathl=[("e1", ("p1", "1"))],
        )
        ev.source.path[-1].relationship = {}
        self.write_binary_events([ev])
