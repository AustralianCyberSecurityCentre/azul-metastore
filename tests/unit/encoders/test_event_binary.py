import copy
import json
import os

from azul_bedrock import models_network as azm

from azul_metastore.common.feature import FeatureEncodeException, enrich_feature
from azul_metastore.encoders import base_encoder
from azul_metastore.encoders import binary2 as esc
from tests.support import gen, unit_test


class TestTimePartition(unit_test.BaseUnitTestCase):
    def setUp(self) -> None:
        os.environ["metastore_sources"] = json.dumps(
            {
                "s0": {},
                "s1": {},
                "s2": {"partition_unit": "day"},
                "s3": {"partition_unit": "month"},
                "s4": {"partition_unit": "year"},
                "s6": {"irrelevant": "corrupt"},
            }
        )

    def test_index_extension(self):
        self.assertEqual("2010-01-02", base_encoder.partition_format("2010-01-02T10:01:01", "day"))
        self.assertEqual("2010-01-02", base_encoder.partition_format("2010-01-02", "day"))
        self.assertEqual("2010-01", base_encoder.partition_format("2010-01-02", "month"))
        self.assertEqual("2010", base_encoder.partition_format("2010-01-02", "year"))
        self.assertRaises(Exception, base_encoder.partition_format, *("2010-01-02", "bfbf"))
        self.assertRaises(Exception, base_encoder.partition_format, *("2010-01-02", ""))


class TestBinaryEventEncode(unit_test.BaseUnitTestCase):
    @classmethod
    def alter_environment(cls):
        super().alter_environment()
        os.environ["metastore_sources"] = json.dumps(
            {
                "generic_source": {},
                "s0": {},
                "s1": {},
                "s2": {"partition_unit": "day"},
                "s3": {"partition_unit": "month"},
                "s4": {"partition_unit": "year"},
                "s6": {"irrelevant": "corrupt"},
            }
        )

    def test_rewrite_id(self):
        data = gen.binary_event(model=False, eid="1")
        data["kafka_key"] = "pizza"
        data = esc.Binary2.encode(data)
        self.assertNotIn("kafka_key", data)
        self.assertIn("_id", data)

    def test_routing(self):
        data = gen.binary_event(model=False, eid="1")
        data["kafka_key"] = "pizza"
        data["entity"]["sha256"] = "1234"
        data = esc.Binary2.encode(data)
        self.assertEqual("1234", data["_routing"])

    def test_no_flags(self):
        data = gen.binary_event(model=False, eid="1")
        data["kafka_key"] = "pizza"
        data["flags"] = ["nowrite", "autodrop", "priority"]
        data = esc.Binary2.encode(data)
        self.assertNotIn("flags", data)

    def test_encode_security1(self):
        data = gen.binary_event(model=False, eid="1")
        data["kafka_key"] = "pizza"
        data["author"]["security"] = "MEDIUM REL:APPLE"
        data["source"]["security"] = "MEDIUM REL:APPLE"
        for node in data["source"]["path"]:
            node["author"]["security"] = "MEDIUM REL:APPLE"
        data = esc.Binary2.encode(data)
        self.assertEqual(set(data["encoded_security"]["exclusive"]), {"s-medium"})
        self.assertEqual(set(data["encoded_security"]["inclusive"]), {"s-rel-apple"})

    def test_encode_security2(self):
        data = gen.binary_event(model=False, eid="1")
        data["kafka_key"] = "pizza"
        data["author"]["security"] = "LOW"
        data["source"]["security"] = "LOW"
        for node in data["source"]["path"]:
            node["author"]["security"] = "LOW"
        data = esc.Binary2.encode(data)
        self.assertEqual(set(data["encoded_security"]["exclusive"]), {"s-low"})
        self.assertEqual(set(data["encoded_security"]["inclusive"]), {"s-any"})

    def test_encode_security3(self):
        data = gen.binary_event(model=False, eid="1")
        data["kafka_key"] = "pizza"
        data["author"]["security"] = "MEDIUM REL:APPLE"
        data["source"]["security"] = "HIGH REL:APPLE"
        for node in data["source"]["path"]:
            node["author"]["security"] = "MEDIUM REL:APPLE"
        data = esc.Binary2.encode(data)
        self.assertEqual(set(data["encoded_security"]["exclusive"]), {"s-high"})
        self.assertEqual(set(data["encoded_security"]["inclusive"]), {"s-rel-apple"})

    def test_encode_feature(self):
        data = gen.binary_event(model=False, eid="pizza", fvtl=[("feature1", "1", "integer")])
        data = esc.Binary2.encode(data)
        self.assertEqual("feature1", data["features"][0]["name"])

    def test_encode_source_data(self):
        data = gen.binary_event(model=False, eid="1")
        data["kafka_key"] = "pizza"
        data["source"]["references"] = {
            "fruit": "apple",
            "colour": "blue",
        }
        data = esc.Binary2.encode(data)
        self.assertEqual(
            {
                "fruit": "apple",
                "colour": "blue",
            },
            data["source"]["references"],
        )
        self.assertEqual(
            {"key": "colour", "value": "blue", "key_value": "colour.blue"}, data["source"]["encoded_references"][0]
        )
        self.assertEqual(
            {"key": "fruit", "value": "apple", "key_value": "fruit.apple"}, data["source"]["encoded_references"][1]
        )

    def test_timestamp_utc(self):
        t1 = "2021-07-25T22:22:50+0250"
        t2 = "2021-07-25T22:22:50+0100"
        t3 = "2021-07-25T22:22:50+02:10"
        data = gen.binary_event(model=False, eid="1")
        data["kafka_key"] = "pizza"
        data["timestamp"] = t1
        data["source"]["timestamp"] = t2
        for node in data["source"]["path"]:
            node["timestamp"] = t3
        data = esc.Binary2.encode(data)
        self.assertEqual("2021-07-25T19:32:50Z", data["timestamp"])

    def test_stream_sort(self):
        data = gen.binary_event(model=False, eid="1")
        data["kafka_key"] = "pizza"
        data["entity"]["datastreams"] = [gen.data(hash="cccc"), gen.data(hash="aaaa"), gen.data(hash="bbbb")]
        data = esc.Binary2.encode(data)
        self.assertTrue(data["datastreams"][0]["sha256"].endswith("aaaa"))
        self.assertTrue(data["datastreams"][1]["sha256"].endswith("bbbb"))
        self.assertTrue(data["datastreams"][2]["sha256"].endswith("cccc"))

    def test_encode_features(self):
        data = gen.binary_event(
            model=False,
            eid="pizza",
            features=[
                gen.feature(fv=("f2", "2")),
                gen.feature(fv=("f1", "1")),
                gen.feature(fv=("f1", "2")),
                gen.feature(fv=("f2", "1")),
                gen.feature(fv=("f2", "3")),
            ],
        )
        data = esc.Binary2.encode(data)
        self.assertEqual(2, data["num_feature_names"])
        self.assertEqual(5, data["num_feature_values"])

    def test_enriching_uri_features(self):
        """Verify given Uris with a double port 9006:9006 enrichment fails but features are still encoded."""
        with self.assertRaises(FeatureEncodeException):
            f1 = gen.feature(fvt=("f1", "http://random:9006:9006/submit.php", azm.FeatureType.Uri.value))
            enrich_feature(f1)

        with self.assertRaises(FeatureEncodeException):
            f2 = gen.feature(fvt=("f2", "https://other.com:443:443/api/x", azm.FeatureType.Uri.value))
            enrich_feature(f2)

        # Verify that the features are still encoded, even though they aren't enriched.
        data = gen.binary_event(
            model=False,
            eid="pizza",
            features=[
                gen.feature(gen.feature(fvt=("f1", "http://random:9006:9006/submit.php", azm.FeatureType.Uri.value))),
                gen.feature(gen.feature(fvt=("f2", "https://other.com:443:443/api/x", azm.FeatureType.Uri.value))),
            ],
        )
        data = esc.Binary2.encode(data)
        self.assertEqual(2, data["num_feature_names"])
        # Features are both still encoded.
        self.assertEqual(2, data["num_feature_values"])

    def test_depth(self):
        data = gen.binary_event(model=False, eid="pizza", spathl=[("10", None), ("10", None)])
        data = esc.Binary2.encode(data)
        self.assertEqual(2, data["depth"])

        data = gen.binary_event(
            model=False,
            eid="pizza",
            spathl=[
                ("10", None),
            ],
        )
        data = esc.Binary2.encode(data)
        self.assertEqual(1, data["depth"])

        data = gen.binary_event(model=False, eid="pizza", spathl=[("10", None) for _ in range(100)])
        data = esc.Binary2.encode(data)
        self.assertEqual(100, data["depth"])

    def test_author(self):
        # enriched
        data = gen.binary_event(
            model=False,
            eid="pizza",
            action="enriched",
            authornv=("gusto", "1"),
            spathl=[("10", None), ("pizza", None)],
        )
        data = esc.Binary2.encode(data)
        self.assertEqual("gusto", data["author"]["name"])
        self.assertEqual("1", data["author"]["version"])

        # sourced
        data = gen.binary_event(
            model=False,
            eid="pizza",
            action="sourced",
            authornv=("gusto", "1"),
            spathl=[("10", None), ("alt", None)],
        )
        data = esc.Binary2.encode(data)
        self.assertEqual("gusto", data["author"]["name"])
        self.assertEqual("1", data["author"]["version"])

    def test_parent_with_path(self):
        data = gen.binary_event(
            model=False,
            eid="1",
            authornv=("a3", "1"),
            spathl=[
                ("10", ("a1", "999.99.9")),
                ("10", ("a1", "1")),
                ("10", ("a2", "1")),
            ],
        )
        data["source"]["path"][3]["action"] = "enriched"
        data = esc.Binary2.encode(data)
        self.assertEqual("10", data["parent"]["sha256"])
        self.assertEqual({"random": "data", "action": "extracted", "label": "within"}, data["parent"]["relationship"])
        self.assertEqual("plugin.a2.1", data["parent_track_author"])

    def test_aggs(self):
        data = gen.binary_event(model=False, eid="pizza", spathl=[("10", ("a1", "1"))])
        data = esc.Binary2.encode(data)
        self.assertEqual("pizza.plugin.generic_plugin.extracted", data["sha256_author_action"])
        self.assertEqual("pizza", data["sha256"])
        self.assertEqual("10", data["parent"]["sha256"])
        self.assertEqual(1, data["depth"])


class TestBinaryEventFiltering(unit_test.BaseUnitTestCase):
    @classmethod
    def alter_environment(cls):
        super().alter_environment()
        os.environ["metastore_sources"] = json.dumps(
            {
                "generic_source": {},
                "s0": {},
                "s1": {},
                "s2": {"partition_unit": "day"},
                "s3": {"partition_unit": "month"},
                "s4": {"partition_unit": "year"},
                "s6": {"irrelevant": "corrupt"},
            }
        )

    def test_no_filtering(self):
        data1 = gen.binary_event(
            model=False,
            eid="pizza1",
            features=[
                gen.feature(fv=("f2", "2")),
                gen.feature(fv=("f1", "1")),
                gen.feature(fv=("f1", "2")),
                gen.feature(fv=("f2", "1")),
                gen.feature(fv=("f2", "3")),
            ],
            authornv=("plugin1", "1.1"),
            action=azm.BinaryAction.Enriched,
        )
        data2 = gen.binary_event(
            model=False,
            eid="pizza2",
            spathl=[("10", ("a1", "1"))],
            authornv=("plugin1", "1.1"),
            action=azm.BinaryAction.Enriched,
        )
        data3 = gen.binary_event(
            model=False,
            eid="pizza3",
            spathl=[("10", ("a1", "1"))],
            authornv=("plugin1", "1.1"),
            action=azm.BinaryAction.Enriched,
        )
        data4 = gen.binary_event(
            model=False, eid="pizza4", authornv=("plugin1", "1.1"), action=azm.BinaryAction.Enriched
        )

        for cur_event in [data1, data2, data3, data4]:
            encoded = esc.Binary2.encode(cur_event)
            after_filtering = esc.Binary2.filter_seen_and_create_parent_events(encoded)
            # Should contain this event + the parent event.
            self.assertEqual(2, len(after_filtering))

    def test_filtering(self):
        """Verify filtering works when it is expected to."""
        data1 = gen.binary_event(
            model=False,
            eid="pizza",
            features=[
                gen.feature(fv=("f2", "2")),
                gen.feature(fv=("f1", "1")),
                gen.feature(fv=("f1", "2")),
                gen.feature(fv=("f2", "1")),
                gen.feature(fv=("f2", "3")),
            ],
            authornv=("plugin4", "1.2"),
            action=azm.BinaryAction.Enriched,
        )
        data2 = copy.deepcopy(data1)
        data1 = esc.Binary2.encode(data1)
        after_filtering = esc.Binary2.filter_seen_and_create_parent_events(data1)
        # Should contain this event + the parent event.
        self.assertEqual(2, len(after_filtering))

        # Verify re-inserting the same events get filtered out
        data2 = esc.Binary2.encode(data2)
        after_filtering = esc.Binary2.filter_seen_and_create_parent_events(data2)
        self.assertEqual(0, len(after_filtering))

        # Slightly different event but same parent event, the main event is filtered out.
        data3 = gen.binary_event(
            model=False,
            eid="pizza",
            authornv=("plugin4", "1.2"),
            action=azm.BinaryAction.Extracted,
        )
        data3 = esc.Binary2.encode(data3)
        after_filtering = esc.Binary2.filter_seen_and_create_parent_events(data3)
        self.assertEqual(1, len(after_filtering))

    def test_filtering_mapped(self):
        """Mapped events are never filtered out, sending the same event doesn't do anything."""
        data = gen.binary_event(
            model=False,
            eid="pizza",
            spathl=[("10", ("a1", "1"))],
            authornv=("plugin1", "1.3"),
            action=azm.BinaryAction.Mapped,
        )

        for cur_event in [copy.deepcopy(data), copy.deepcopy(data), copy.deepcopy(data), copy.deepcopy(data)]:
            encoded = esc.Binary2.encode(cur_event)
            after_filtering = esc.Binary2.filter_seen_and_create_parent_events(encoded)
            # Should contain this event + the parent event.
            self.assertEqual(2, len(after_filtering))

    def test_filter_sources(self):
        data1 = gen.binary_event(
            model=False,
            eid="pizza",
            action=azm.BinaryAction.Sourced,
            sourceit=("s2", "2000-01-01T01:01:01Z"),
        )
        data2 = gen.binary_event(
            model=False,
            eid="pizza",
            action=azm.BinaryAction.Sourced,
            sourceit=("s1", "2000-01-01T01:01:01Z"),
        )
        data3 = gen.binary_event(
            model=False,
            eid="pizza",
            action=azm.BinaryAction.Sourced,
            sourceit=("s1", "2000-01-01T01:01:01Z"),
        )
        data1 = esc.Binary2.encode(data1)
        after_filtering = esc.Binary2.filter_seen_and_create_parent_events(data1)
        # Should contain this event + the parent event.
        self.assertEqual(2, len(after_filtering))

        data2 = esc.Binary2.encode(data2)
        after_filtering = esc.Binary2.filter_seen_and_create_parent_events(data2)
        self.assertEqual(1, len(after_filtering))

        data3 = esc.Binary2.encode(data3)
        after_filtering = esc.Binary2.filter_seen_and_create_parent_events(data3)
        self.assertEqual(0, len(after_filtering))


class TestBinaryEventDecode(unit_test.BaseUnitTestCase):
    def test_feature_location(self):
        data = gen.binary_event(model=False, eid="1")
        data["entity"]["features"] = [gen.feature(fv=("1", "1"), patch={"offset": 123, "size": 12})]

        data = esc.Binary2.encode(data)
        self.assertEqual({"gte": 123, "lte": 135}, data["features"][0]["encoded"]["location"])
        data = esc.Binary2.decode(data)

        self.assertEqual(123, data["entity"]["features"][0]["offset"])
        self.assertEqual(12, data["entity"]["features"][0]["size"])
        self.assertNotIn("encoded", data["entity"]["features"][0])

    def test_security(self):
        data = gen.binary_event(model=False, eid="1")
        data["kafka_key"] = "pizza"
        data["author"]["security"] = "LOW"
        data["source"]["security"] = "LOW"
        for node in data["source"]["path"]:
            node["author"]["security"] = "LOW"
        data = esc.Binary2.encode(data)
        self.assertEqual("LOW", data["security"])
        self.assertEqual(
            {"exclusive": ["s-low"], "inclusive": ["s-any"], "markings": ["s-any"], "num_exclusive": 1},
            data["encoded_security"],
        )
        data = esc.Binary2.decode(data)
        self.assertNotIn("security", data)
        self.assertNotIn("encoded_security", data)
        self.assertNotIn("encoded_security", data["author"])
        self.assertEqual("LOW", data["author"]["security"])
        self.assertEqual("LOW", data["source"]["security"])
        self.assertEqual("LOW", data["source"]["path"][0]["author"]["security"])

    def test_source_data(self):
        data = gen.binary_event(model=False, eid="1")
        data["kafka_key"] = "pizza"
        data["source"]["references"] = {"a": "1", "b": "2", "c": "3"}
        data = esc.Binary2.encode(data)
        self.assertNotEqual({"a": "1", "b": "2", "c": "3"}, data["source"]["encoded_references"])
        self.assertEqual({"a": "1", "b": "2", "c": "3"}, data["source"]["references"])
        data = esc.Binary2.decode(data)
        self.assertEqual({"a": "1", "b": "2", "c": "3"}, data["source"]["references"])
        self.assertEqual([], data["source"].get("encoded_references", []))
