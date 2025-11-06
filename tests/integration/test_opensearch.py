import os
from unittest import mock

from azul_metastore import settings
from azul_metastore.common import manager
from azul_metastore.encoders import base_encoder
from tests.support import integration_test


def nc(_uid, col):
    return {
        "uid": _uid,
        "colour": col,
    }


class ATester(base_encoder.BaseIndexEncoder):
    docname = "doc1"
    mapping = {
        "dynamic": "strict",
        "properties": {
            "uid": {"type": "keyword"},
            "angle": {"type": "integer"},
            "colours": {
                "properties": {
                    "uid": {"type": "keyword"},
                    "colour": {"type": "keyword"},
                },
                "type": "nested",
            },
            "tags": {"type": "keyword"},
            "security": {"type": "keyword"},
            "encoded_security": {"type": "object", "enabled": False},
        },
    }


class TestSearch(integration_test.DynamicTestCase):
    def setUp(self):
        self.system.delete_indexes()
        self.el = ATester()
        self.el.w.initialise(self.system.writer.sd, force=True)
        self.flush()

    def test_read_write_doc(self):
        uid = "12345"
        doc = {
            "uid": uid,
            "_id": uid,
            "colours": [
                nc("1", "green"),
                nc("2", "pink"),
            ],
            "security": "LOW",
            "encoded_security": {"exclusive": ["s-low"]},
        }

        self.el.w.wrap_and_index_docs(self.system.writer.sd, [doc])
        self.flush()

        resp = self.el.w.search(self.system.writer.sd, body={"query": {"bool": {"filter": [{"term": {"uid": uid}}]}}})
        ret = resp["hits"]["hits"][0]["_source"]

        self.assertEqual(ret["uid"], "12345")
        self.assertEqual(len(ret["colours"]), 2)

    def test_update_doc(self):
        uid = "12345"
        odoc = {
            "_id": uid,
            "uid": uid,
            "colours": [nc("1", "green"), nc("2", "pink")],
            "angle": 200,
            "tags": ["animal"],
            "security": "LOW",
            "encoded_security": {"exclusive": ["s-low"]},
        }
        ndoc = {
            "_id": uid,
            "uid": uid,
            "colours": [nc("3", "purple"), nc("1", "green")],
            "angle": 200,
            "tags": ["fungus", "animal"],
            "security": "LOW",
            "encoded_security": {"exclusive": ["s-low"]},
        }
        self.el.w.wrap_and_index_docs(self.system.writer.sd, [odoc, ndoc])
        self.flush()

        resp = self.el.w.search(self.system.writer.sd, body={"query": {"bool": {"filter": [{"term": {"uid": uid}}]}}})
        ret = resp["hits"]["hits"][0]["_source"]

        self.assertEqual(ret["uid"], "12345")
        self.assertEqual(ret["angle"], 200)

        cols = {x["colour"] for x in ret["colours"]}
        self.assertEqual(len(cols), 2)
        self.assertEqual(cols, {"purple", "green"})


class TestManager(integration_test.DynamicTestCase):

    # Disable cache for test case.
    @mock.patch("azul_metastore.settings.get", settings.Metastore)
    def setUp(self):
        super().setUp()
        os.environ["METASTORE_PLUGIN_INDEX_CONFIG"] = (
            '{"number_of_shards": 8, "number_of_replicas": 4, "refresh_interval": "2s"}'
        )
        os.environ["METASTORE_STATUS_INDEX_CONFIG"] = (
            '{"number_of_shards": 10, "number_of_replicas": 1, "refresh_interval": "7s"}'
        )
        self.man = manager.Manager()
        self.man.initialise(self.writer.sd)

    @classmethod
    def tearDownClass(cls) -> None:
        del os.environ["METASTORE_PLUGIN_INDEX_CONFIG"]
        del os.environ["METASTORE_STATUS_INDEX_CONFIG"]
        return super().tearDownClass()

    def test_settings(self):
        """Verify the settings for shards and replicas came through appropriately."""
        # Check the settings on the encoder
        print(self.man.plugin.index_settings)
        self.assertEqual(
            self.man.plugin.index_settings, {"number_of_shards": 8, "number_of_replicas": 4, "refresh_interval": "2s"}
        )

        print(self.man.status.index_settings)
        self.assertEqual(
            self.man.status.index_settings, {"number_of_shards": 10, "number_of_replicas": 1, "refresh_interval": "7s"}
        )

        # Check the settings on the wrapper inside the encoder
        self.assertEqual(
            self.man.plugin.w.index_settings,
            {"number_of_shards": 8, "number_of_replicas": 4, "refresh_interval": "2s"},
        )

        self.assertEqual(
            self.man.status.w.index_settings,
            {"number_of_shards": 10, "number_of_replicas": 1, "refresh_interval": "7s"},
        )

    def test_get_settings(self):
        plugin_template = self.man.plugin.w.get_template(self.writer.sd)
        print(plugin_template)
        self.assertEqual(plugin_template.get("settings").get("index").get("number_of_shards"), "8")
        self.assertEqual(plugin_template.get("settings").get("index").get("number_of_replicas"), "4")
        self.assertEqual(plugin_template.get("settings").get("index").get("refresh_interval"), "2s")

        status_template = self.man.status.w.get_template(self.writer.sd)
        print(status_template)
        self.assertEqual(status_template.get("settings").get("index").get("number_of_shards"), "10")
        self.assertEqual(status_template.get("settings").get("index").get("number_of_replicas"), "1")
        self.assertEqual(status_template.get("settings").get("index").get("refresh_interval"), "7s")
