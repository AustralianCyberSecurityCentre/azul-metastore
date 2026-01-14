from azul_bedrock import models_restapi

from azul_metastore.query import annotation, cache, plugin, status
from azul_metastore.query.binary2 import (
    binary_feature,
    binary_find,
    binary_read,
    binary_related,
    binary_similar,
    binary_source,
    binary_summary,
)
from tests.support import integration_test


class TestEmpty(integration_test.DynamicTestCase):
    def test_binary_read(self):
        self.assertEqual((False, None, None), binary_read.find_stream_references(self.writer, "e1"))
        self.assertFalse(binary_read.check_binaries(self.writer, ["e1"])[0]["exists"])
        self.assertFalse(binary_find.find_binaries(self.writer, hashes=["e1"], count_binaries=True).items_count, 0)
        self.assertFalse(self.read_binary_events("e1"))
        self.assertFalse(self.read_binary_events("e1"))
        self.assertFalse(binary_find.find_binaries(self.writer, hashes=["e1"]).items[0].exists)
        self.assertFalse(binary_find.find_binaries(self.writer, hashes=["e1"], count_binaries=True).items_count, 0)
        self.assertFalse(binary_read.get_binary_newer(self.writer, "e1", "2000-01-01T00:00:00Z").count)
        self.assertFalse(binary_summary.read(self.writer, "e1").security)
        self.assertFalse(binary_summary.read(self.writer, "e1").sources)
        self.assertFalse(binary_summary.read(self.writer, "e1").features)
        self.assertFalse(binary_summary.read(self.writer, "e1").info)
        self.assertFalse(binary_summary.read(self.writer, "e1").streams)
        self.assertFalse(binary_summary.read(self.writer, "e1").instances)
        self.assertEqual((None, None), binary_read.find_stream_metadata(self.writer, "e1", "blah"))
        self.assertFalse(binary_read.get_author_stats(self.writer, "p1", "1"))

    def test_empty_binary_find(self):
        self.assertFalse(binary_find.find_binaries(self.writer).items)

    def test_binary_feature(self):
        self.assertFalse(binary_feature.feature_count_tags(self.writer, ["f1"]))
        self.assertFalse(binary_feature.count_values_in_features(self.writer, ["f1"])[0].values)
        self.assertFalse(binary_feature.count_binaries_with_feature_names(self.writer, ["f1"])[0].entities)
        self.assertFalse(
            binary_feature.count_binaries_with_feature_values(
                self.writer, [models_restapi.ValueCountItem(name="f1", value="v1")]
            )[0].entities
        )
        cache.invalidate_all(self.writer)
        self.flush()
        self.assertFalse(
            binary_feature.count_binaries_with_part_values(
                self.writer, [models_restapi.ValuePartCountItem(part="hostname", value="v1")]
            )[0].entities
        )
        self.assertFalse(binary_feature.find_feature_values(self.writer, "f1").values)

    def test_binary_related(self):
        self.assertFalse(binary_related.read_children(self.writer, "e1"))
        self.assertFalse(binary_summary.read(self.writer, "e1").parents)
        self.assertFalse(binary_related.read_nearby(self.writer, "e1").links)

    def test_binary_similar(self):
        gen = binary_similar.read_similar_from_features(self.writer, "e1")
        next(gen)
        self.assertFalse(next(gen)["matches"])

    def test_plugin(self):
        self.assertFalse(plugin.get_all_plugins_full(self.writer))
        self.assertFalse(plugin.get_all_plugins(self.writer))
        self.assertFalse(plugin.get_plugin(self.writer, "p1", "1"))
        self.assertEqual(["filename", "magic", "mime"], plugin.get_raw_feature_names(self.writer))
        self.assertFalse(plugin.find_features(self.writer))

    def test_source(self):
        self.assertFalse(binary_source.read_source_references(self.writer, "s1"))
        self.assertFalse(binary_source.read_source(self.writer, "s1")["num_entities"])

    def test_status(self):
        self.assertFalse(status.get_statuses(self.writer, "e1"))
        self.assertFalse(status._get_opensearch_binary_status(self.writer, "e1"))
        self.assertFalse(status.get_binary_status(self.writer, "e1"))
        self.assertFalse(plugin.get_author_stats(self.writer, "p1", "1"))

    def test_annotation(self):
        self.assertFalse(annotation.read_binary_tags(self.writer, "e1"))
        self.assertFalse(annotation.read_all_binary_tags(self.writer).num_tags)
