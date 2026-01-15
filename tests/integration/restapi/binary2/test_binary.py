from unittest import mock

from azul_bedrock import models_network as azm
from azul_bedrock.models_restapi import binaries as bedr_binaries

from azul_metastore import settings
from azul_metastore.context import Context
from tests.support import gen, integration_test


def mock_user_security(self):
    """Fake method for getting users security context."""
    return "LOW"


@mock.patch.object(Context, "get_user_current_security", mock_user_security)
class TestBinary(integration_test.BaseRestapi):
    def test_status_find(self):
        self.write_status_events([gen.status(eid="e1")])
        response = self.client.get("/v0/binaries/e1/statuses")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(0, resp["data"]["items"][0]["completed"])

    def test_entity_check_exists(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1"),
            ]
        )
        response = self.client.head("/v0/binaries/e1")
        self.assertEqual(200, response.status_code)

        response = self.client.head("/v0/binaries/E1")
        self.assertEqual(200, response.status_code)

        response = self.client.head("/v0/binaries/invalid1")
        self.assertEqual(404, response.status_code)

    def test_entity_read(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1"),
            ]
        )

        # Without the detail flag
        response = self.client.get("/v0/binaries/e1")
        self.assertEqual(200, response.status_code)
        resp = response.json()

        # Assert that all expected objects exist within the response
        self.assertEqual(1, resp["data"]["documents"]["count"])
        self.assertEqual(1, len(resp["data"]["security"]))

        self.assertEqual(1, len(resp["data"]["sources"]))
        self.assertEqual("generic_source", resp["data"]["sources"][0]["source"])

        self.assertEqual([], resp["data"]["tags"])
        self.assertEqual([], resp["data"]["parents"])

        self.assertEqual(1, len(resp["data"]["instances"]))
        self.assertEqual("sourced", resp["data"]["instances"][0]["action"])

        self.assertGreater(len(resp["data"]["features"]), 0)

        self.assertEqual(1, len(resp["data"]["streams"]))
        self.assertEqual(["content"], resp["data"]["streams"][0]["label"])

        self.assertEqual([], resp["data"]["info"])

    def test_binary_plugin_zero_filtering(self):
        # Test to check if plugins that specify filter_max_content_size of 0 are are not filtered/in the warnings list
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", post_patch={"entity": {"size": 1024}}, data_patch={"size": 1024}),
                gen.binary_event(eid="e2", post_patch={"entity": {"size": 102400000}}, data_patch={"size": 102400000}),
            ]
        )

        # Configure test plugins with filter size of 0
        plugin_id = ("a1", "1")
        plugin_config = {"filter_max_content_size": "0"}

        self.write_plugin_events(
            [
                gen.plugin({"timestamp": "2021-01-01T12:00+00:00"}, authornv=plugin_id, config=plugin_config),
            ]
        )

        response = self.client.get("/v0/binaries/e1")
        self.assertEqual(200, response.status_code)
        resp = response.json()

        self.assertNotIn("diagnostics", resp["data"])

    def test_binary_plugin_zero_filtering_bad_plugin_configuration(self):
        # Test to check if plugins that specify filter_max_content_size of 0 are are not filtered/in the warnings list
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", post_patch={"entity": {"size": 1024}}, data_patch={"size": 1024}),
                gen.binary_event(eid="e2", post_patch={"entity": {"size": 102400000}}, data_patch={"size": 102400000}),
            ]
        )

        # Configure test plugins with filter size not set
        plugin_id = ("a1", "1")
        plugin_config = {"oh_no_no_filter_max_content_siiiiizeee": "0"}

        self.write_plugin_events(
            [
                gen.plugin({"timestamp": "2021-01-01T12:00+00:00"}, authornv=plugin_id, config=plugin_config),
            ]
        )

        response = self.client.get("/v0/binaries/e1")
        self.assertEqual(200, response.status_code)
        resp = response.json()

        self.assertNotIn("diagnostics", resp["data"])

    def test_binary_read_detail(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", post_patch={"entity": {"size": 1024}}, data_patch={"size": 1024}),
                gen.binary_event(eid="e2", post_patch={"entity": {"size": 102400000}}, data_patch={"size": 102400000}),
            ]
        )

        # Configure test plugins with filter sizes
        plugin_id = ("a1", "1")
        plugin_bad_config = {"filter_max_content_size": "1023"}
        plugin_config = {"filter_max_content_size": "1024"}

        self.write_plugin_events(
            [
                # Should always select the newest:
                gen.plugin(timestamp="2020-01-01T12:00+00:00", authornv=plugin_id, config=plugin_bad_config),
                gen.plugin(timestamp="2021-01-01T12:00+00:00", authornv=plugin_id, config=plugin_config),
            ]
        )

        # Small binary
        response = self.client.get("/v0/binaries/e1")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        print("RESPONSE:")
        print(resp)

        # This would be expected to error if the older plugin config is picked
        self.assertNotIn("diagnostics", resp["data"])

        # Larger binary
        response = self.client.get("/v0/binaries/e2")
        self.assertEqual(200, response.status_code)
        resp = response.json()

        self.assertEqual(1, len(resp["data"]["diagnostics"]))
        self.assertEqual("warning", resp["data"]["diagnostics"][0]["severity"])
        self.assertEqual("large", resp["data"]["diagnostics"][0]["id"])
        self.assertIn("a1", resp["data"]["diagnostics"][0]["body"])

    def test_binary_many_events(self):
        s = settings.get()

        # Generate many events for the same binary that don't overlap in IDs
        events = [gen.binary_event(eid="e1", authornv=(f"test.{id}", "1")) for id in range(0, s.warn_on_event_count)]
        self.write_binary_events(events)

        # Larger binary
        response = self.client.get("/v0/binaries/e1")
        self.assertEqual(200, response.status_code)
        resp = response.json()

        self.assertEqual(1, len(resp["data"]["diagnostics"]))
        self.assertEqual("warning", resp["data"]["diagnostics"][0]["severity"])
        self.assertEqual("many_events", resp["data"]["diagnostics"][0]["id"])
        self.assertIn(str(s.warn_on_event_count), resp["data"]["diagnostics"][0]["body"])

    def test_binary_read_new(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1"),
            ]
        )
        response = self.client.get("/v0/binaries/e1/new?timestamp=2000-01-01T12:00%2B00:00")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(True, bool(resp["data"]["count"]))

        response = self.client.get("/v0/binaries/E1/new?timestamp=2000-01-01T12:00%2B00:00")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(True, bool(resp["data"]["count"]))

        response = self.client.get("/v0/binaries/e1/new?timestamp=2100-01-01T12:00%2B00:00")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(False, bool(resp["data"]["count"]))

    def test_binary_read_new_query_logs(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1"),
            ]
        )
        response = self.client.get("/v0/binaries/e1/new?timestamp=2000-01-01T12:00:00&include_queries=true")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, len(resp["meta"]["queries"]))

    def test_binary_read_similar_ssdeep(self):
        hash_same = "192:9p2BHR9woCmKMGRZUCsA7knmxs1yxXMdYKMNbp:j2BHR3DGQ1msyx8M"
        hash_added = (
            "384:p2BHR3DGQknzQnzm5l4nnmsyxlc4vLhkAvLhaAzLZcGvLZ0GvLZ0GvLZ0GvLZcFM:peWnzQnzm5l4nnCckhkshaWZcCZ0CZ0w"
        )
        hash_removed = (
            "96:9pETyYBP1U35TR0gggX7DOspzoMRQRAFyy8i3PNtbRmyFDynmxgPnnIKuKkB9/hD:9p2BuG2sA7knmxs1yxXMdYKMNbp"
        )
        hash_swapped = "192:0R9woCmKMGT1yxYZUCsA7knmxwp2BTMdYKMNbp:0R3DG5yxz1ma2BwM"
        hash_different = "192:9p2BHGxhLAIGvqMo2csjaIjUh+XsDOspzoAMBO:l2PLR3HGF3vdyd1M"

        self.write_binary_events(
            [
                gen.binary_event(eid="same", ssdeep=hash_same),
                gen.binary_event(eid="added", ssdeep=hash_added),
                gen.binary_event(eid="removed", ssdeep=hash_removed),
                gen.binary_event(eid="swapped", ssdeep=hash_swapped),
                gen.binary_event(eid="different", ssdeep=hash_different),
            ]
        )

        response = self.client.get(f"/v0/binaries/similar/ssdeep?ssdeep={hash_same}&max_matches=2")
        self.assertEqual(200, response.status_code)
        matches = response.json()["data"]["matches"]

        self.assertEqual(
            matches,
            [
                {"sha256": "removed", "score": 79},
                {"sha256": "swapped", "score": 72},
            ],
        )

        # check that lowercase provides different results
        response = self.client.get(f"/v0/binaries/similar/ssdeep?ssdeep={hash_same.lower()}")
        self.assertEqual(200, response.status_code)
        matches = response.json()["data"]["matches"]
        self.assertEqual(len(matches), 2)

    def test_binary_read_similar_tlsh(self):
        hash_base = "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        hash_mod_a = "T1BBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        hash_mod_b = "T1CCCCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        self.write_binary_events(
            [
                gen.binary_event(eid="same", tlsh=hash_base),
                gen.binary_event(eid="mod_a", tlsh=hash_mod_a),
                gen.binary_event(eid="mod_b", tlsh=hash_mod_b),
                gen.binary_event(eid="unrelated_null", tlsh="TNULL"),
            ]
        )

        response = self.client.get(f"/v0/binaries/similar/tlsh?tlsh={hash_base}&max_matches=2")
        self.assertEqual(200, response.status_code)
        matches = response.json()["data"]["matches"]

        self.assertEqual(
            matches,
            [
                {"sha256": "mod_a", "score": 99.92},
                {"sha256": "mod_b", "score": 99.42},
            ],
        )

    def test_binary_read_similar(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvl=[("f1", f"v{x}") for x in range(10)]),
                gen.binary_event(eid="e2", fvl=[("f1", f"v{x}") for x in range(10)]),
            ]
        )
        response = self.client.get("/v0/binaries/e1/similar")
        self.assertEqual(200, response.status_code)
        resp = response.json()

        response = self.client.get("/v0/binaries/E1/similar")
        self.assertEqual(200, response.status_code)
        resp = response.json()

    def test_binary_read_nearby(self):
        """ "Test relationship with the following binary relationship structure.

        Paths are as follows: arrow works like this Parent -> Child
        generic_source_1 -> e1 -> e10 -> e100 -> e1000
        generic_source_2 -> e2 -> e20 -> e200 -> e1000
        also e10 -> e300
        """
        a = ("a1", "1")
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", spathl=[]),
                gen.binary_event(eid="e10", spathl=[("e1", a)]),
                gen.binary_event(eid="e100", spathl=[("e1", a), ("e10", a)]),
                gen.binary_event(eid="e2", spathl=[]),
                gen.binary_event(eid="e20", spathl=[("e2", a)]),
                gen.binary_event(eid="e200", spathl=[("e2", a), ("e20", a)]),
                gen.binary_event(eid="e1000", authornv=("a2", "1"), spathl=[("e2", a), ("e20", a), ("e200", a)]),
                gen.binary_event(eid="e1000", authornv=("a1", "1"), spathl=[("e1", a), ("e10", a), ("e100", a)]),
                gen.binary_event(eid="e300", spathl=[("e1", a), ("e10", a)]),
            ]
        )
        # Base case, links are parent: generic_source, e1, e10 child: e1000, cousins: e300, e200, e20
        response = self.client.get("/v0/binaries/e100/nearby")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(7, len(resp["data"]["links"]))

        response = self.client.get("/v0/binaries/E100/nearby")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(7, len(resp["data"]["links"]))

        # Check cousins
        response = self.client.get("/v0/binaries/e100/nearby?include_cousins=yes")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(7, len(resp["data"]["links"]))

        # Find only parents, and children
        response = self.client.get("/v0/binaries/e100/nearby?include_cousins=no")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(4, len(resp["data"]["links"]))

        # Verify the search also works with the small option missing out the wider cousins.
        response = self.client.get("/v0/binaries/e100/nearby?include_cousins=yes_small")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(6, len(resp["data"]["links"]))

        # Widen search to also find cousins: e2 and generic_sourced2
        response = self.client.get("/v0/binaries/e100/nearby?include_cousins=yes_large")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(9, len(resp["data"]["links"]))

    def test_binary_read_tags(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1"),
            ]
        )
        self.write_entity_tags(
            [
                gen.entity_tag(eid="e1", tag="t1"),
                gen.entity_tag(eid="e1", tag="t2"),
            ]
        )
        response = self.client.get("/v0/binaries/e1/tags")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(2, len(resp["data"]["items"]))
        response = self.client.get("/v0/binaries/E1/tags")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(2, len(resp["data"]["items"]))

    def test_entity_tags_create_delete(self):
        response = self.client.post("/v0/binaries/e1/tags/hello.world", json=dict(security="low"))
        self.assertEqual(400, response.status_code)
        response = self.client.post(
            "/v0/binaries/e1/tags/helloworldhelloworldhelloworldhelloworldhelloworldhelloworld",
            json=dict(security="low"),
        )
        self.assertEqual(400, response.status_code)

        response = self.client.post("/v0/binaries/e1/tags/t1", json=dict(security="low"))
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(None, resp)

        response = self.client.get("/v0/binaries/tags")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, len(resp["data"]["tags"]))
        self.assertEqual("t1", resp["data"]["tags"][0]["tag"])

        response = self.client.delete("/v0/binaries/e1/tags/t1")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        # Check that the tag was "updated" as we are not deleting tags due to audit
        self.assertEqual(1, resp["data"]["updated"])

        response = self.client.get("/v0/binaries/tags")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(0, len(resp["data"]["tags"]))

    def test_entity_tag_casing(self):
        response = self.client.post("/v0/binaries/E1/tags/t1", json=dict(security="low"))
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(None, resp)

        response = self.client.get("/v0/binaries/e1/tags")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, len(resp["data"]["items"]))
        self.assertEqual("e1", resp["data"]["items"][0]["sha256"])
        self.assertEqual("t1", resp["data"]["items"][0]["tag"])

        response = self.client.get("/v0/binaries/E1/tags")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, len(resp["data"]["items"]))
        self.assertEqual("e1", resp["data"]["items"][0]["sha256"])
        self.assertEqual("t1", resp["data"]["items"][0]["tag"])

        response = self.client.delete("/v0/binaries/E1/tags/t1")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        # Check that the tag was "updated" as we are not deleting tags due to audit
        self.assertEqual(1, resp["data"]["updated"])

        response = self.client.get("/v0/binaries/E1/tags")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual([], resp["data"]["items"])

        response = self.client.get("/v0/binaries/e1/tags")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual([], resp["data"]["items"])

    def test_entity_model(self):
        response = self.client.get("/v0/binaries/model")
        self.assertEqual(200, response.status_code)
        resp = response.json()

        # OpenSearch's state doesn't necessarily reset, so comparing against the original
        # list might not work
        # Test an individual sample of features:
        server_pairs = resp["data"]["keys"]

        self.assertEqual(server_pairs["sha256_author_action"], "keyword")
        self.assertEqual(server_pairs["author.name"], "keyword")
        self.assertEqual(server_pairs["size"], "unsigned_long")
        self.assertEqual(server_pairs["magic"], "keyword")

    def test_security_tag(self):
        response = self.client.post("/v0/binaries/E1/tags/t1", json=dict(security="high"))
        self.assertEqual(200, response.status_code)
        response = self.client.post("/v0/binaries/E1/tags/t1", json=dict(security="medium"))
        self.assertEqual(200, response.status_code)

        user = "low"
        response = self.client.post(
            "/v0/binaries/E1/tags/t1", json=dict(security="high"), headers={"x-test-user": user}
        )
        self.assertEqual(422, response.status_code)
        response = self.client.post(
            "/v0/binaries/E1/tags/t1", json=dict(security="medium"), headers={"x-test-user": user}
        )
        self.assertEqual(422, response.status_code)

    def test_binary_get_all_documents(self):
        """Verify API endpoint filters different Opensearch documents by the "action" field."""
        self.write_binary_events(
            [
                gen.binary_event(
                    {"action": azm.BinaryAction.Sourced},
                    eid="e1",
                    sourceit=("s2", "2021-02-01T11:00:00+00:00"),
                ),
                gen.binary_event(
                    {"action": azm.BinaryAction.Sourced},
                    eid="e1",
                    sourceit=("s3", "2021-03-01T11:00:00+00:00"),
                ),
                gen.binary_event(
                    {"action": azm.BinaryAction.Sourced},
                    eid="e1",
                    sourceit=("s4", "2021-04-01T11:00:00+00:00"),
                ),
                gen.binary_event(
                    {"action": azm.BinaryAction.Mapped}, eid="e1", sourceit=("s5", "2021-05-01T11:00:00+00:00")
                ),
            ]
        )

        response = self.client.get(f"/v0/binaries/e1/events?event_type={azm.BinaryAction.Sourced}")
        self.assertEqual(200, response.status_code)
        resp = bedr_binaries.OpensearchDocuments(**response.json()["data"])
        self.assertEqual(3, len(resp.items))
        self.assertEqual(3, resp.total_docs)

        response = self.client.get(f"/v0/binaries/e1/events?event_type={azm.BinaryAction.Mapped}")
        self.assertEqual(200, response.status_code)
        resp = bedr_binaries.OpensearchDocuments(**response.json()["data"])
        self.assertEqual(1, len(resp.items))
        self.assertEqual(1, resp.total_docs)

        response = self.client.get("/v0/binaries/e1/events")
        self.assertEqual(200, response.status_code)
        resp = bedr_binaries.OpensearchDocuments(**response.json()["data"])
        self.assertEqual(4, len(resp.items))
        self.assertEqual(4, resp.total_docs)

        response = self.client.get("/v0/binaries/e1/events?size=1")
        self.assertEqual(200, response.status_code)
        resp = bedr_binaries.OpensearchDocuments(**response.json()["data"])
        self.assertEqual(1, len(resp.items))
        self.assertEqual(4, resp.total_docs)

        response = self.client.get("/v0/binaries/e2/events")
        self.assertEqual(404, response.status_code)

    def test_binary_autocomplete(self):
        response = self.client.get("/v0/binaries/autocomplete?term=&offset=0")
        self.assertEqual(200, response.status_code)
        resp = response.json()["data"]
        self.assertFormatted(resp, {"type": "Initial"})

        response = self.client.get("/v0/binaries/autocomplete?term=test%3A&offset=4")
        self.assertEqual(200, response.status_code)
        resp = response.json()["data"]
        self.assertFormatted(
            resp,
            {
                "type": "FieldValue",
                "key": "test",
                "prefix": "(search for if specified field exists - add a value to search for a value)",
                "prefix_type": "empty",
            },
        )

        response = self.client.get("/v0/binaries/autocomplete?term=test%3A%5B5%20TO%2010%5D&offset=9")
        self.assertEqual(200, response.status_code)
        resp = response.json()["data"]
        self.assertFormatted(
            resp,
            {"type": "FieldValue", "key": "test", "prefix": "5 (inclusive) to 10 (inclusive)", "prefix_type": "range"},
        )
