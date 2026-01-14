from multiprocessing.pool import ThreadPool

from azul_metastore.query import annotation
from tests.support import gen, integration_test


class TestBinaryFind(integration_test.BaseRestapi):
    def test_binary_find_simple(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1"),
                gen.binary_event(eid="e2"),
                gen.binary_event(eid="e3"),
                gen.binary_event(eid="e4"),
                gen.binary_event(eid="e5"),
                gen.binary_event(eid="e6"),
                gen.binary_event(eid="e7"),
                gen.binary_event(eid="e8"),
                gen.binary_event(eid="e9"),
            ]
        )
        response = self.client.get("/v0/binaries?max_entities=10")
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        self.assertEqual(9, len(parsed["data"]["items"]))

        response = self.client.get("/v0/binaries?max_entities=5")
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        self.assertEqual(5, len(parsed["data"]["items"]))

        response = self.client.get(
            "/v0/binaries?"
            "sort_asc=true&"
            "term=beef&"
            "time_source=2020-01-01T01:01:01&"
            "source=43&"
            "sources_not=1&"
            "source_depth=5&"
            "source_depth_not=0&"
            "source_refs=123&"
            "source_refs_unique=54392867094&"
            "author=blah&"
            "author_not=blah2&"
            "features=elf&"
            "features_not=exe&"
            "feature_values=blah.blah&"
            "feature_values_not=blah3424.blah&"
            "parts=54312.654&"
            "parts_not=097650.654&"
            "max_entities=500"
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(0, len(resp["data"]["items"]))

    def test_binary_find_term(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1"),
                gen.binary_event(eid="e2"),
                gen.binary_event(eid="e3"),
            ]
        )
        response = self.client.get("/v0/binaries?term=e1&max_entities=500")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, len(resp["data"]["items"]))
        response = self.client.get("/v0/binaries?term=E1&max_entities=500")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, len(resp["data"]["items"]))

    def test_binary_find_term_query_logs(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1"),
                gen.binary_event(eid="e2"),
                gen.binary_event(eid="e3"),
            ]
        )
        response = self.client.get("/v0/binaries?term=e1&max_entities=500")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, len(resp["data"]["items"]))
        self.assertIsNone(resp["meta"].get("queries"))

        response = self.client.get("/v0/binaries?term=e1&max_entities=500&include_queries=true")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, len(resp["data"]["items"]))
        self.assertEqual(3, len(resp["meta"]["queries"]))

        expected = [
            {
                "query_type": "search",
                "query": {
                    "query": {
                        "bool": {
                            "filter": [
                                {"has_child": {"type": "metadata", "query": {"exists": {"field": "source.name"}}}},
                                {
                                    "has_child": {
                                        "type": "metadata",
                                        "query": {
                                            "bool": {
                                                "should": [
                                                    {"term": {"sha256": {"value": "e1", "boost": 20}}},
                                                    {"prefix": {"sha256": "e1"}},
                                                    {"prefix": {"md5": "e1"}},
                                                    {"prefix": {"sha1": "e1"}},
                                                    {"prefix": {"sha512": "e1"}},
                                                    {"prefix": {"ssdeep.hash": "E1"}},
                                                    {"prefix": {"file_format": "E1"}},
                                                    {"prefix": {"magic": "E1"}},
                                                    {"prefix": {"mime": "E1"}},
                                                ],
                                                "minimum_should_match": 1,
                                            }
                                        },
                                        "inner_hits": {
                                            "_source": False,
                                            "name": "1",
                                            "highlight": {
                                                "order": "score",
                                                "fields": {"*": {}},
                                                "pre_tags": [""],
                                                "post_tags": [""],
                                                "encoder": "default",
                                            },
                                        },
                                    }
                                },
                            ],
                            "should": [],
                            "must_not": [],
                        }
                    },
                    "sort": [{"_score": {"order": "desc"}}],
                    "size": 500,
                    "_source": False,
                    "track_total_hits": False,
                },
                "args": None,
                "kwargs": None,
            },
            {
                "query_type": "search",
                "query": {
                    "query": {"bool": {"filter": [{"terms": {"sha256": ["e1"]}}], "must_not": [], "must": []}},
                    "size": 0,
                    "_source": False,
                    "aggs": {
                        "SHA256": {
                            "terms": {"field": "sha256", "size": 1},
                            "aggs": {
                                "SOURCE": {
                                    "terms": {"field": "source.name", "size": 3},
                                    "aggs": {
                                        "SUMMARY": {
                                            "top_hits": {
                                                "size": 1,
                                                "_source": [
                                                    "depth",
                                                    "source.name",
                                                    "source.references",
                                                    "source.timestamp",
                                                    "file_format",
                                                    "file_extension",
                                                    "size",
                                                    "magic",
                                                    "mime",
                                                    "md5",
                                                    "sha1",
                                                    "sha256",
                                                    "sha512",
                                                    "tlsh",
                                                    "ssdeep",
                                                    "datastreams.label",
                                                    "filename",
                                                ],
                                                "sort": [
                                                    {"filename": "desc"},
                                                    {"file_format": "desc"},
                                                    {"mime": "desc"},
                                                    {"magic": "desc"},
                                                    {"datastreams.label": "asc"},
                                                    {"depth": "asc"},
                                                    {"source.timestamp": "desc"},
                                                ],
                                            }
                                        }
                                    },
                                }
                            },
                        }
                    },
                },
                "args": None,
                "kwargs": None,
            },
            {
                "query_type": "search",
                "query": {
                    "query": {
                        "bool": {
                            "filter": [{"term": {"type": "entity_tag"}}, {"terms": {"sha256": ["e1"]}}],
                            "must_not": [{"term": {"state": "disabled"}}],
                            "must": [],
                        }
                    },
                    "sort": ["tag"],
                    "size": 100,
                },
                "args": None,
                "kwargs": None,
            },
        ]

        def _fix_runtime(_resp):
            for q in _resp:
                q.pop("run_time_ms", None)
                q.pop("index", None)
                # don't care about inspecting the response
                q.pop("response", None)

        _fix_runtime(expected)

        # Run the query a few times to ensure the number of include_queries doesn't change
        response = self.client.get(
            "/v0/binaries?term=E1&max_entities=500&include_queries=true", headers={"x-test-user": "low"}
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        # blat runtimes
        _fix_runtime(resp["meta"]["queries"])
        self.assertFormatted(resp["meta"]["queries"], expected)
        self.assertEqual(3, len(resp["meta"]["queries"]))
        response = self.client.get(
            "/v0/binaries?term=E1&max_entities=500&include_queries=true", headers={"x-test-user": "low"}
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(3, len(resp["meta"]["queries"]))
        _fix_runtime(resp["meta"]["queries"])
        self.assertFormatted(resp["meta"]["queries"], expected)  # check same still
        response = self.client.get(
            "/v0/binaries?term=E1&max_entities=500&include_queries=true", headers={"x-test-user": "low"}
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(3, len(resp["meta"]["queries"]))
        self.assertEqual(1, len(resp["data"]["items"]))
        _fix_runtime(resp["meta"]["queries"])
        self.assertFormatted(resp["meta"]["queries"], expected)  # check same still

        # Run concurrent queries to ensure there isn't weird interactions between concurrent queries
        # this will happen if you clear the query data list everytime you create context.
        def make_api_request(client):
            return client.get("/v0/binaries?term=E1&max_entities=500&include_queries=true")

        # Run it five times to ensure you don't just get lucky and happen to get 2 responses.
        for _ in range(5):
            pool = ThreadPool(4)
            results = pool.map(make_api_request, [self.client, self.client, self.client, self.client, self.client])
            pool.close()
            pool.join()
            for result in results:
                resp = result.json()
                self.assertEqual(3, len(resp["meta"]["queries"]))

    def test_binary_find_free_text(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1"),
                gen.binary_event(eid="e2"),
                gen.binary_event(eid="e3"),
            ]
        )

        # Search just for 'e1'
        response = self.client.get("/v0/binaries", params={"term": "e1", "max_entities": 500})
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, len(resp["data"]["items"]))

        # Search for 'e1' OR 'e2'
        response = self.client.get("/v0/binaries", params={"term": "e1 OR e2", "max_entities": 500})
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(2, len(resp["data"]["items"]))

    def test_binary_find_sort(self):
        # Sort order is (oldest to newest date)
        # timestamp        - e2, e1, e3
        # source.timestamp - e1, e3, e2
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1", timestamp="2024-04-01T01:01:01Z", sourceit=("generic_source", "2021-01-01T01:01:01Z")
                ),
                gen.binary_event(
                    eid="e2", timestamp="2023-05-01T01:01:01Z", sourceit=("generic_source", "2023-03-01T01:01:01Z")
                ),
                gen.binary_event(
                    eid="e3", timestamp="2025-02-01T01:01:01Z", sourceit=("generic_source", "2022-02-01T01:01:01Z")
                ),
            ]
        )
        response = self.client.get("/v0/binaries?sort=_score&sort_asc=False")
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        self.assertEqual(3, len(parsed["data"]["items"]))
        self.assertEqual(
            parsed["data"]["items"][0],
            {
                "key": "e3",
                "exists": True,
                "has_content": True,
                "highlight": {},
                "sources": [
                    {
                        "depth": 0,
                        "name": "generic_source",
                        "timestamp": "2022-02-01T01:01:01Z",
                        "references": {"ref1": "val1", "ref2": "val2"},
                    }
                ],
                "file_size": 1024,
                "file_format": "text/plain",
                "file_extension": "txt",
                "magic": "ASCII text",
                "mime": "text/plain",
                "md5": "000000000000000000000000000000e3",
                "sha1": "00000000000000000000000000000000000000e3",
                "sha256": "e3",
                "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e3",
                "ssdeep": "1:1:1",
                "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
            },
        )

        response = self.client.get("/v0/binaries?sort=_score&sort_asc=True")
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        self.assertEqual(3, len(parsed["data"]["items"]))

        # Sort order is (oldest to newest date)
        # timestamp        - e2, e1, e3
        # source.timestamp - e1, e3, e2
        response = self.client.get("/v0/binaries?sort=source.timestamp&sort_asc=False")
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        resp_itemes = parsed["data"]["items"]
        self.assertEqual(3, len(resp_itemes))
        self.assertEqual(resp_itemes[0]["key"], "e2")
        self.assertEqual(resp_itemes[1]["key"], "e3")
        self.assertEqual(resp_itemes[2]["key"], "e1")

        response = self.client.get("/v0/binaries?sort=source.timestamp&sort_asc=True")
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        resp_itemes = parsed["data"]["items"]
        self.assertEqual(3, len(resp_itemes))
        self.assertEqual(resp_itemes[0]["key"], "e1")
        self.assertEqual(resp_itemes[1]["key"], "e3")
        self.assertEqual(resp_itemes[2]["key"], "e2")

        response = self.client.get("/v0/binaries?sort=timestamp&sort_asc=False")
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        resp_itemes = parsed["data"]["items"]
        self.assertEqual(3, len(resp_itemes))
        self.assertEqual(resp_itemes[0]["key"], "e3")
        self.assertEqual(resp_itemes[1]["key"], "e1")
        self.assertEqual(resp_itemes[2]["key"], "e2")

        response = self.client.get("/v0/binaries?sort=timestamp&sort_asc=True")
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        resp_itemes = parsed["data"]["items"]
        self.assertEqual(3, len(resp_itemes))
        self.assertEqual(resp_itemes[0]["key"], "e2")
        self.assertEqual(resp_itemes[1]["key"], "e1")
        self.assertEqual(resp_itemes[2]["key"], "e3")

    def test_binary_find_tag(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e2", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e3", fvl=[("f1", "v2")]),
            ]
        )
        self.write_entity_tags(
            [
                gen.entity_tag(eid="e1", tag="t1"),
                gen.entity_tag(eid="e2", tag="t1"),
                gen.entity_tag(eid="e5", tag="t1"),
            ]
        )

        response = self.client.get("/v0/binaries?term=binary.tag:t1&max_entities=500")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(2, len(resp["data"]["items"]))

        response = self.client.get("/v0/binaries?term=binary.tag:invalid1&max_entities=500")
        self.assertEqual(400, response.status_code)
        resp = response.json()
        self.assertEqual("tag not found", resp["detail"])

    def test_binary_find_tag_hundreds(self):
        """Test binary tags still works when there is 800 tags, this can fail if the Opensearch
        query isn't done correctly.
        """
        all_events = []
        all_tags = []
        first_id = 20
        # 9999, is used to tag enough binary events the query fails under certain conditions.
        for current_event_id in range(9999):
            all_events.append(gen.binary_event(eid=f"e{current_event_id + first_id}", fvl=[("f1", "v1")]))
            all_tags.append(
                gen.entity_tag(eid=f"e{current_event_id + first_id}", tag="t2"),
            )

        self.write_binary_events(all_events)
        self.write_entity_tags(all_tags)

        response = self.client.get("/v0/binaries?term=binary.tag:t2&max_entities=800")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(800, len(resp["data"]["items"]))

    def test_remove_binary_tag_and_validate(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e2", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e3", fvl=[("f1", "v2")]),
            ]
        )
        self.write_entity_tags(
            [
                gen.entity_tag(eid="e1", tag="t1"),
                gen.entity_tag(eid="e2", tag="t1"),
            ]
        )
        # Remove the tag on one of the entities then check only 1 binary with `t1` is found
        annotation.delete_binary_tag(self.system.writer, "e2", "t1")
        self.flush()

        response = self.client.get("/v0/binaries?term=binary.tag:t1&max_entities=500")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, len(resp["data"]["items"]))

    def test_binary_find_fv_tag(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e2", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e3", fvl=[("f1", "v2")]),
            ]
        )
        self.write_fv_tags(
            [
                gen.feature_value_tag(fv=("f1", "v1"), tag="t1"),
            ]
        )
        response = self.client.get("/v0/binaries?term=feature.tag:t1&max_entities=100")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(2, len(resp["data"]["items"]))

        response = self.client.get("/v0/binaries?term=feature.tag:invalid1&max_entities=100")
        self.assertEqual(400, response.status_code)
        resp = response.json()
        self.assertEqual("feature value tag not found", resp["detail"])

    def test_exclude_certain_security_labels(self):
        self.write_binary_events(
            [
                # All have the Author LOW TLP:CLEAR
                gen.binary_event(eid="e11", sourcesec=gen.g1_1),  # "LOW TLP:CLEAR",
                gen.binary_event(eid="e12", sourcesec=gen.g2_1),  # "MEDIUM REL:APPLE",
                gen.binary_event(eid="e13", sourcesec=gen.g3_12),  # "MEDIUM MOD1 REL:APPLE REL:BEE",
                gen.binary_event(eid="e14", sourcesec=gen.g1_1 + " TLP:GREEN"),  # "LOW TLP:GREEN",
            ]
        )
        # Baseline response with no filtering
        response = self.client.get("/v0/binaries")
        self.assertEqual(200, response.status_code)
        response_data = response.json()
        self.assertEqual(len(response_data["data"]["items"]), 4)

        response = self.client.get("/v0/binaries?x=MEDIUM&x=HIGH")
        self.assertEqual(200, response.status_code)
        response_data = response.json()
        self.assertEqual(len(response_data["data"]["items"]), 2)
        for item in response_data["data"]["items"]:
            self.assertIn(item["key"], ["e11", "e14"])
        self.assertEqual(
            response_data["meta"]["security"],
            "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR",
        )
        self.assertEqual(
            response.headers.get("x-azul-security"),
            "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR",
        )

        response = self.client.get("/v0/binaries?x=TOP%20HIGH&x=REL:APPLE")
        self.assertEqual(200, response.status_code)
        response_data = response.json()
        self.assertEqual(len(response_data["data"]["items"]), 2)
        self.assertEqual(response_data["meta"]["security"], "HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:BEE,CAR")
        self.assertEqual(
            response.headers.get("x-azul-security"),
            "HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:BEE,CAR",
        )

        response = self.client.get("/v0/binaries?x=MOD1")
        self.assertEqual(200, response.status_code)
        response_data = response.json()
        self.assertEqual(len(response_data["data"]["items"]), 3)
        for item in response_data["data"]["items"]:
            self.assertIn(item["key"], ["e11", "e12", "e14"])
        self.assertEqual(response_data["meta"]["security"], "TOP HIGH MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR")
        self.assertEqual(
            response.headers.get("x-azul-security"),
            "TOP HIGH MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR",
        )

        response = self.client.get("/v0/binaries?x=LOW")
        self.assertEqual(200, response.status_code)
        response_data = response.json()
        self.assertEqual(len(response_data["data"]["items"]), 2)
        for item in response_data["data"]["items"]:
            self.assertIn(item["key"], ["e12", "e13"])
        self.assertEqual(
            response_data["meta"]["security"],
            "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR",
        )
        self.assertEqual(
            response.headers.get("x-azul-security"),
            "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR",
        )

        response = self.client.get("/v0/binaries?x=TLP:GREEN")
        self.assertEqual(200, response.status_code)
        response_data = response.json()
        self.assertEqual(len(response_data["data"]["items"]), 3)
        for item in response_data["data"]["items"]:
            self.assertIn(item["key"], ["e11", "e12", "e13"])
        self.assertEqual(
            response_data["meta"]["security"],
            "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR",
        )
        self.assertEqual(
            response.headers.get("x-azul-security"),
            "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR",
        )

        response = self.client.get("/v0/binaries?x=LOW&x=MEDIUM&x=HIGH&x=TOP%20HIGH&x=TLP:AMBER%2BSTRICT")
        self.assertEqual(200, response.status_code)
        response_data = response.json()
        self.assertEqual(len(response_data["data"]["items"]), 0)
        self.assertEqual(
            response_data["meta"]["security"],
            "LOW: LY MOD1 MOD2 MOD3 HANOVERLAP OVER TLP:AMBER",
        )
        self.assertEqual(
            response.headers.get("x-azul-security"),
            "LOW: LY MOD1 MOD2 MOD3 HANOVERLAP OVER TLP:AMBER",
        )
