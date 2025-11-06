import datetime
import json
import os

from azul_bedrock.models_restapi import sources as bedr_sources

from azul_metastore import settings
from azul_metastore.query.binary2 import binary_source
from tests.support import gen, integration_test


class TestSearchEntity(integration_test.DynamicTestCase):

    @classmethod
    def alter_environment(cls):
        super().alter_environment()
        os.environ["metastore_sources"] = json.dumps(
            {
                "s1": {
                    "expire_events_after": "1 days",
                    "partition_unit": "day",
                    "elastic": {
                        "number_of_shards": 2,
                        "number_of_replicas": 0,
                    },
                    "references": [
                        {"name": "r1", "required": True, "description": ""},
                        {"name": "r2", "required": True, "description": ""},
                    ],
                },
                "s2": {
                    "partition_unit": "year",
                },
                "s3": {},
                "s4": {},
                "s5": {},
                "generic_source": {},
                "infinity": {
                    "partition_unit": "all",
                },
            }
        )

    def test_read_sources_simple(self):
        sources = binary_source.read_sources()
        self.assertEqual(7, len(sources))

        self.assertIn("s1", sources)
        self.assertIn("s2", sources)

        _source = binary_source.read_sources()["s1"]
        self.assertSetEqual({"r1", "r2"}, {x["name"] for x in _source["references"]})
        _source = binary_source.read_sources()["s2"]
        self.assertSetEqual(set(), {x["name"] for x in _source["references"]})

    def test_read_source(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", sourceit=("s1", "2000-01-01T00:00:00Z")),
                gen.binary_event(eid="e1", sourceit=("s2", "2000-01-02T00:00:00Z")),
            ]
        )
        _source = binary_source.read_source(self.writer, "s1")
        self.assertEqual("2000-01-01T00:00:00.000Z", _source["newest"])
        self.assertEqual(1, _source["num_entities"])
        _source = binary_source.read_source(self.writer, "s2")
        self.assertEqual("2000-01-02T00:00:00.000Z", _source["newest"])
        self.assertEqual(1, _source["num_entities"])

    def test_read_source_references(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1", sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"r1": "123", "r2": "321"}
                ),
                gen.binary_event(eid="e1", sourceit=("s2", "2000-01-02T00:00:00Z"), sourcerefs={}),
            ]
        )
        refs = binary_source.read_source_references(self.writer, "s1")
        self.assertEqual(1, len(refs))
        self.assertSetEqual({"r1", "r2"}, set(refs[0].values.keys()))
        self.assertSetEqual({"123", "321"}, set(refs[0].values.values()))

        refs = binary_source.read_source_references(self.writer, "s2")
        self.assertEqual(1, len(refs))
        self.assertSetEqual(set(), set(refs[0].values.keys()))
        self.assertSetEqual(set(), set(refs[0].values.values()))

    def test_read_source_submissions(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1", sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"r1": "123", "r2": "321"}
                ),
                gen.binary_event(
                    eid="e2", sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"r1": "123", "r2": "321"}
                ),
                gen.binary_event(
                    eid="e3", sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"r1": "DDD", "r2": "321"}
                ),
                gen.binary_event(
                    eid="e1", sourceit=("s1", "2000-01-02T00:00:00Z"), sourcerefs={"r1": "123", "r2": "321"}
                ),
                gen.binary_event(
                    eid="e1", sourceit=("s1", "2000-01-03T00:00:00Z"), sourcerefs={"r1": "123", "r2": "321"}
                ),
                gen.binary_event(eid="e1", sourceit=("s2", "2000-01-04T00:00:00Z"), sourcerefs={}),
            ]
        )

        # --- Full listing of potential sources
        refs = binary_source.read_submissions(self.writer, "s1")
        # 3 as the first e1,e2 have the same sourcerefs and timestamps so they will be one submission.
        self.assertEqual(
            refs,
            [
                bedr_sources.ReferenceSet(
                    track_source_references="s1.1ac6dbe66ba36e8b6888dd6daf521792",
                    timestamp="2000-01-03T00:00:00.000Z",
                    num_entities=1,
                    num_entities_min=False,
                    values={"r2": "321", "r1": "123"},
                ),
                bedr_sources.ReferenceSet(
                    track_source_references="s1.1ac6dbe66ba36e8b6888dd6daf521792",
                    timestamp="2000-01-02T00:00:00.000Z",
                    num_entities=1,
                    num_entities_min=False,
                    values={"r2": "321", "r1": "123"},
                ),
                bedr_sources.ReferenceSet(
                    track_source_references="s1.1ac6dbe66ba36e8b6888dd6daf521792",
                    timestamp="2000-01-01T00:00:00.000Z",
                    num_entities=2,
                    num_entities_min=False,
                    values={"r2": "321", "r1": "123"},
                ),
                bedr_sources.ReferenceSet(
                    track_source_references="s1.ae0210e1091b51278de5d8545a665911",
                    timestamp="2000-01-01T00:00:00.000Z",
                    num_entities=1,
                    num_entities_min=False,
                    values={"r2": "321", "r1": "DDD"},
                ),
            ],
        )

        # --- Get submissions at a specific timestamp (may be more than one)
        refs = binary_source.read_submissions(
            self.writer, "s1", submission_timestamp=datetime.datetime.fromisoformat("2000-01-01T00:00:00Z")
        )
        self.assertEqual(
            refs,
            [
                bedr_sources.ReferenceSet(
                    track_source_references="s1.1ac6dbe66ba36e8b6888dd6daf521792",
                    timestamp="2000-01-01T00:00:00.000Z",
                    num_entities=2,
                    num_entities_min=False,
                    values={"r2": "321", "r1": "123"},
                ),
                bedr_sources.ReferenceSet(
                    track_source_references="s1.ae0210e1091b51278de5d8545a665911",
                    timestamp="2000-01-01T00:00:00.000Z",
                    num_entities=1,
                    num_entities_min=False,
                    values={"r2": "321", "r1": "DDD"},
                ),
            ],
        )

        # --- Get submissions with specific reference (more than one)
        refs = binary_source.read_submissions(
            self.writer, "s1", track_source_references="s1.1ac6dbe66ba36e8b6888dd6daf521792"
        )
        self.assertEqual(
            refs,
            [
                bedr_sources.ReferenceSet(
                    track_source_references="s1.1ac6dbe66ba36e8b6888dd6daf521792",
                    timestamp="2000-01-03T00:00:00.000Z",
                    num_entities=1,
                    num_entities_min=False,
                    values={"r2": "321", "r1": "123"},
                ),
                bedr_sources.ReferenceSet(
                    track_source_references="s1.1ac6dbe66ba36e8b6888dd6daf521792",
                    timestamp="2000-01-02T00:00:00.000Z",
                    num_entities=1,
                    num_entities_min=False,
                    values={"r2": "321", "r1": "123"},
                ),
                bedr_sources.ReferenceSet(
                    track_source_references="s1.1ac6dbe66ba36e8b6888dd6daf521792",
                    timestamp="2000-01-01T00:00:00.000Z",
                    num_entities=2,
                    num_entities_min=False,
                    values={"r2": "321", "r1": "123"},
                ),
            ],
        )

        # --- Get exact submission should only be one of them containing all files.
        refs = binary_source.read_submissions(
            self.writer,
            "s1",
            submission_timestamp=datetime.datetime.fromisoformat("2000-01-01T00:00:00Z"),
            track_source_references="s1.1ac6dbe66ba36e8b6888dd6daf521792",
        )
        self.assertFormatted(
            refs,
            [
                bedr_sources.ReferenceSet(
                    track_source_references="s1.1ac6dbe66ba36e8b6888dd6daf521792",
                    timestamp="2000-01-01T00:00:00.000Z",
                    num_entities=2,
                    num_entities_min=False,
                    values={"r2": "321", "r1": "123"},
                )
            ],
        )

        # --- Verify alterative source works as expected
        refs = binary_source.read_submissions(self.writer, "s2")
        self.assertEqual(1, len(refs))
        self.assertSetEqual(set(), set(refs[0].values.keys()))
        self.assertSetEqual(set(), set(refs[0].values.values()))

    def test_check_source_exists(self):
        self.assertTrue(settings.check_source_exists("s1"))
        self.assertTrue(settings.check_source_exists("s2"))
        self.assertFalse(settings.check_source_exists("htrdhtyfb"))
        self.assertFalse(settings.check_source_exists("gfrd56ey56756"))

    def test_read_sources(self):
        sources = binary_source.read_sources()
        self.assertLess(0, len(sources))

        self.assertIn("s1", sources)
        self.assertIn("s2", sources)
        self.assertIn("s3", sources)
        self.assertIn("s4", sources)
        self.assertIn("s5", sources)
        self.assertIn("generic_source", sources)

    def test_read_source_verifying_order(self):
        es = self.writer
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", sourceit=("s1", "2000-01-01T00:00:00Z")),
                gen.binary_event(eid="e1", sourceit=("s2", "2000-01-02T00:00:00Z")),
                gen.binary_event(eid="e2", sourceit=("s2", "2000-01-03T00:00:00Z")),
                gen.binary_event(eid="e1", sourceit=("s3", "2000-01-04T00:00:00Z")),
                gen.binary_event(eid="e2", sourceit=("s3", "2000-01-05T00:00:00Z")),
                gen.binary_event(eid="e3", sourceit=("s3", "2000-01-06T00:00:00Z")),
            ]
        )

        _source = binary_source.read_source(es, "s1")
        self.assertEqual("2000-01-01T00:00:00.000Z", _source["newest"])
        self.assertEqual(1, _source["num_entities"])

        _source = binary_source.read_source(es, "s2")
        self.assertEqual("2000-01-03T00:00:00.000Z", _source["newest"])
        self.assertEqual(2, _source["num_entities"])

        _source = binary_source.read_source(es, "s3")
        self.assertEqual("2000-01-06T00:00:00.000Z", _source["newest"])
        self.assertEqual(3, _source["num_entities"])

    def test_add_to_invalid_source(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", sourceit=("invalid1", "2000-01-01T00:00:00Z")),
                gen.binary_event(eid="e2", sourceit=("invalid2", "2000-01-01T00:00:00Z")),
            ],
            must_error=2,
        )
