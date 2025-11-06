import copy

from tests.support import gen, integration_test


class TestSources(integration_test.BaseRestapi):
    def test_sources_read(self):
        self.write_binary_events([gen.binary_event(sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"ref1": "a"})])
        response = self.client.get("/v0/sources/")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(6, len(resp["data"]))
        self.assertIn("s1", resp["data"])

    def test_source_exist(self):
        self.write_binary_events([gen.binary_event(sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"ref1": "a"})])
        response = self.client.head("/v0/sources/invalid1")
        self.assertEqual(404, response.status_code)

        response = self.client.head("/v0/sources/s1")
        self.assertEqual(200, response.status_code)

    def test_source(self):
        self.write_binary_events([gen.binary_event(sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"ref1": "a"})])
        response = self.client.get("/v0/sources/invalid1")
        self.assertEqual(404, response.status_code)
        resp = response.json()
        self.assertEqual("Not Found", resp["detail"])

        response = self.client.get("/v0/sources/s1")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual("s1", resp["data"]["name"])
        self.assertEqual(1, resp["data"]["num_entities"])

        response = self.client.get("/v0/sources/s2")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual("s2", resp["data"]["name"])
        self.assertEqual(0, resp["data"]["num_entities"])

    def test_source_query_logs(self):
        self.write_binary_events([gen.binary_event(sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"ref1": "a"})])
        response = self.client.get("/v0/sources/invalid1?include_queries=true")
        self.assertEqual(404, response.status_code)
        resp = response.json()
        self.assertEqual(None, resp.get("meta"))

        response = self.client.get("/v0/sources/s1?include_queries=true")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(3, len(resp["meta"]["queries"]))

    def test_source_references_read(self):
        self.write_binary_events(
            [
                gen.binary_event(sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"ref1": "a"}),
                gen.binary_event(sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"ref1": "abc"}),
                gen.binary_event(sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"ref1": "abcd"}),
                gen.binary_event(sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"ref1": "apple banana"}),
            ]
        )
        response = self.client.get("/v0/sources/s1/references")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        # should be some reference values
        self.assertEqual(4, len(resp["data"]["items"]))
        self.assertLess(0, len(resp["data"]["items"][0]["values"]))

        response = self.client.get("/v0/sources/s1/references?term=abc")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(2, len(resp["data"]["items"]))

        response = self.client.get("/v0/sources/s1/references?term=banana")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, len(resp["data"]["items"]))

        response = self.client.get("/v0/sources/s1/references?term=apple")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, len(resp["data"]["items"]))

    def test_source_submissions_read(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"ref1": "abc"}),
                gen.binary_event(eid="e2", sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"ref1": "abc"}),
                gen.binary_event(eid="e3", sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"ref1": "abc"}),
                gen.binary_event(
                    eid="e4", sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"ref1": "apple banana"}
                ),
                gen.binary_event(
                    eid="e5", sourceit=("s1", "2000-01-02T00:00:00Z"), sourcerefs={"ref1": "apple banana"}
                ),
                gen.binary_event(eid="e6", sourceit=("s1", "2000-01-03T00:00:00Z"), sourcerefs={"ref1": "abc"}),
            ]
        )

        full_expected_response = {
            "data": {
                "items": [
                    {
                        "track_source_references": "s1.67ac3bd180cb26168868980de1eeec7e",
                        "timestamp": "2000-01-03T00:00:00.000Z",
                        "num_entities": 1,
                        "num_entities_min": False,
                        "values": {"ref1": "abc"},
                    },
                    {
                        "track_source_references": "s1.4fafa00a4a688890a41eafad4f4b6c27",
                        "timestamp": "2000-01-02T00:00:00.000Z",
                        "num_entities": 1,
                        "num_entities_min": False,
                        "values": {"ref1": "apple banana"},
                    },
                    {
                        "track_source_references": "s1.4fafa00a4a688890a41eafad4f4b6c27",
                        "timestamp": "2000-01-01T00:00:00.000Z",
                        "num_entities": 1,
                        "num_entities_min": False,
                        "values": {"ref1": "apple banana"},
                    },
                    {
                        "track_source_references": "s1.67ac3bd180cb26168868980de1eeec7e",
                        "timestamp": "2000-01-01T00:00:00.000Z",
                        "num_entities": 3,
                        "num_entities_min": False,
                        "values": {"ref1": "abc"},
                    },
                ]
            },
            "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
        }

        # No filtering works
        response = self.client.get("/v0/sources/s1/submissions")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(resp, full_expected_response)

        # Filtering via timestamp works.
        expected = copy.deepcopy(full_expected_response)
        # keep elements 2,3 (common date)
        del expected["data"]["items"][0]
        del expected["data"]["items"][0]
        response = self.client.get("/v0/sources/s1/submissions?timestamp=2000-01-01T00:00:00Z")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        # should be some reference values
        self.assertFormatted(resp, expected)

        # Filtering via track_source_references only works
        expected = copy.deepcopy(full_expected_response)
        del expected["data"]["items"][1]
        del expected["data"]["items"][1]
        # keep elements 0,3 (common track_source_references)
        response = self.client.get(
            "/v0/sources/s1/submissions?track_source_references=s1.67ac3bd180cb26168868980de1eeec7e"
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(resp, expected)

        # Filtering by track_source_references and timestamp works.
        expected = copy.deepcopy(full_expected_response)
        expected["data"]["items"] = [expected["data"]["items"][3]]
        # keep elements 3 (selected by track_source_references and date)
        response = self.client.get(
            "/v0/sources/s1/submissions?track_source_references=s1.67ac3bd180cb26168868980de1eeec7e&timestamp=2000-01-01T00:00:00Z"
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(resp, expected)
