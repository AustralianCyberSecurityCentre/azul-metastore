from azul_bedrock.models_restapi import features as bedr_features

from tests.support import gen, integration_test


class TestFeatures(integration_test.BaseRestapi):
    def test_entities_counts(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a1", "1"), fvl=[("f1", "v1"), ("f1", "v2"), ("f1", "v3")]),
                gen.binary_event(eid="e2", authornv=("a1", "1"), fvl=[("f1", "v1"), ("f1", "v2")]),
                gen.binary_event(eid="e3", authornv=("a1", "2"), fvl=[("f1", "v1")]),
                gen.binary_event(eid="e4", authornv=("a2", "1"), fvtl=[("f2", "http://blah.com", "uri")]),
                gen.binary_event(eid="e5", authornv=("a2", "1"), fvtl=[("f2", "http://blah.com", "uri")]),
                gen.binary_event(eid="e6", authornv=("a2", "1"), fvtl=[("f2", "https://blah.com", "uri")]),
            ]
        )

        response = self.client.post("/v0/features/entities/counts?author=a1", json=dict(items=["f1", "f2"]))
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(3, resp["data"]["f1"]["entities"])
        self.assertEqual(0, resp["data"]["f2"]["entities"])

        response = self.client.post(
            "/v0/features/entities/counts?author=a1&author_version=1", json=dict(items=["f1", "f2"])
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(2, resp["data"]["f1"]["entities"])
        self.assertEqual(0, resp["data"]["f2"]["entities"])

        response = self.client.post("/v0/features/entities/counts?include_queries=true", json=dict(items=["f1", "f2"]))
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(3, resp["data"]["f1"]["entities"])
        self.assertEqual(3, resp["data"]["f2"]["entities"])
        self.assertEqual(2, len(resp["meta"]["queries"]))

    def test_values_counts(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a1", "2"), fvl=[("f1", "v1"), ("f1", "v2"), ("f1", "v3")]),
                gen.binary_event(eid="e2", authornv=("a1", "1"), fvl=[("f1", "v1"), ("f1", "v2")]),
                gen.binary_event(eid="e3", authornv=("a1", "1"), fvl=[("f1", "v1")]),
                gen.binary_event(eid="e4", authornv=("a2", "1"), fvtl=[("f2", "http://blah.com", "uri")]),
                gen.binary_event(eid="e5", authornv=("a2", "1"), fvtl=[("f2", "http://blah.com", "uri")]),
                gen.binary_event(eid="e6", authornv=("a2", "1"), fvtl=[("f2", "https://blah.com", "uri")]),
            ]
        )

        response = self.client.post("/v0/features/values/counts", json=dict(items=["f1", "f2"]))
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(resp["data"], {"f2": {"name": "f2", "values": 2}, "f1": {"name": "f1", "values": 3}})

        self.flush()
        # test query for uncached feature
        response = self.client.post("/v0/features/values/counts?skip_count=true", json=dict(items=["f1", "f2", "f55"]))
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(resp["data"], {"f2": {"name": "f2", "values": 2}, "f1": {"name": "f1", "values": 3}})

        response = self.client.post("/v0/features/values/counts?author=a1", json=dict(items=["f1", "f2"]))
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(resp["data"], {"f1": {"name": "f1", "values": 3}, "f2": {"name": "f2", "values": 0}})

        response = self.client.post(
            "/v0/features/values/counts?author=a1&author_version=1", json=dict(items=["f1", "f2"])
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(resp["data"], {"f1": {"name": "f1", "values": 2}, "f2": {"name": "f2", "values": 0}})

    def test_feature_values_entities_count(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a1", "1"), fvl=[("f1", "v1"), ("f1", "v2"), ("f1", "v3")]),
                gen.binary_event(eid="e2", authornv=("a2", "1"), fvl=[("f1", "v1"), ("f1", "v2")]),
                gen.binary_event(eid="e3", authornv=("a2", "2"), fvl=[("f1", "v1")]),
                gen.binary_event(eid="e4", authornv=("a2", "1"), fvtl=[("f2", "http://blah.com", "uri")]),
                gen.binary_event(eid="e5", authornv=("a2", "1"), fvtl=[("f2", "http://blah.com", "uri")]),
                gen.binary_event(eid="e6", authornv=("a2", "1"), fvtl=[("f2", "https://blah.com", "uri")]),
                gen.binary_event(
                    eid="e7", authornv=("a1", "1"), fvtl=[("f3", "1.123", "float"), ("f3", "1.234", "float")]
                ),
                gen.binary_event(eid="e8", authornv=("a2", "1"), fvtl=[("f3", "1.123", "float")]),
                gen.binary_event(eid="e9", authornv=("a2", "2"), fvtl=[("f3", "1.123", "float")]),
            ]
        )

        response = self.client.post(
            "/v0/features/values/entities/counts",
            json=dict(items=[dict(name="f1", value="v1"), dict(name="f1", value="v2"), dict(name="f1", value="v3")]),
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(
            resp["data"],
            {
                "f1": {
                    "v1": {"name": "f1", "value": "v1", "entities": 3},
                    "v2": {"name": "f1", "value": "v2", "entities": 2},
                    "v3": {"name": "f1", "value": "v3", "entities": 1},
                }
            },
        )

        response = self.client.post(
            "/v0/features/values/entities/counts?author=a1",
            json=dict(items=[dict(name="f1", value="v1"), dict(name="f1", value="v2"), dict(name="f1", value="v3")]),
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(
            resp["data"],
            {
                "f1": {
                    "v1": {"name": "f1", "value": "v1", "entities": 1},
                    "v2": {"name": "f1", "value": "v2", "entities": 1},
                    "v3": {"name": "f1", "value": "v3", "entities": 1},
                }
            },
        )

        response = self.client.post(
            "/v0/features/values/entities/counts?author=a2&author_version=2",
            json=dict(items=[dict(name="f1", value="v1"), dict(name="f1", value="v2"), dict(name="f1", value="v3")]),
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(
            resp["data"],
            {
                "f1": {
                    "v1": {"name": "f1", "value": "v1", "entities": 1},
                    "v2": {"name": "f1", "value": "v2", "entities": 0},
                    "v3": {"name": "f1", "value": "v3", "entities": 0},
                }
            },
        )

        response = self.client.post(
            "/v0/features/values/entities/counts", json=dict(items=[dict(name="f1", value="invalid1")])
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(resp["data"], {"f1": {"invalid1": {"name": "f1", "value": "invalid1", "entities": 0}}})

        response = self.client.post(
            "/v0/features/values/entities/counts",
            json=dict(items=[dict(name="f2", value="http://blah.com"), dict(name="f2", value="https://blah.com")]),
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(
            resp["data"],
            {
                "f2": {
                    "http://blah.com": {"name": "f2", "value": "http://blah.com", "entities": 2},
                    "https://blah.com": {"name": "f2", "value": "https://blah.com", "entities": 1},
                }
            },
        )

        response = self.client.post(
            "/v0/features/values/entities/counts",
            json=dict(items=[dict(name="f3", value=1.123), dict(name="f3", value=1.234)]),
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(
            resp["data"],
            {
                "f3": {
                    "1.123": {"name": "f3", "value": "1.123", "entities": 3},
                    "1.234": {"name": "f3", "value": "1.234", "entities": 1},
                }
            },
        )

        # ensure cached docs are ready for querying
        self.flush()
        # must not return rows for f4 as it is not cached
        response = self.client.post(
            "/v0/features/values/entities/counts?skip_count=true",
            json=dict(items=[dict(name="f3", value=1.123), dict(name="f3", value=1.234), dict(name="f4", value=899)]),
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(
            resp["data"],
            {
                "f3": {
                    "1.123": {"name": "f3", "value": "1.123", "entities": 3},
                    "1.234": {"name": "f3", "value": "1.234", "entities": 1},
                }
            },
        )

    def test_feature_values_parts_count(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a1", "1"), fvl=[("f1", "v1"), ("f1", "v2"), ("f1", "v3")]),
                gen.binary_event(eid="e2", authornv=("a2", "1"), fvl=[("f1", "v1"), ("f1", "v2")]),
                gen.binary_event(eid="e3", authornv=("a2", "1"), fvl=[("f1", "v1")]),
                gen.binary_event(eid="e4", authornv=("a1", "1"), fvtl=[("f2", "http://blah.com", "uri")]),
                gen.binary_event(eid="e5", authornv=("a2", "1"), fvtl=[("f2", "http://blah.com", "uri")]),
                gen.binary_event(eid="e6", authornv=("a2", "2"), fvtl=[("f2", "https://blah.com", "uri")]),
            ]
        )

        response = self.client.post(
            "/v0/features/values/parts/entities/counts", json=dict(items=[dict(part="hostname", value="blah.com")])
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(3, resp["data"]["blah.com"]["hostname"]["entities"])

        response = self.client.post(
            "/v0/features/values/parts/entities/counts?author=a2&author_version=2",
            json=dict(items=[dict(part="hostname", value="blah.com")]),
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, resp["data"]["blah.com"]["hostname"]["entities"])

        response = self.client.post(
            "/v0/features/values/parts/entities/counts",
            json=dict(items=[dict(part="hostname", value="invalid.org")]),
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(0, resp["data"]["invalid.org"]["hostname"]["entities"])

        # Case where the part isn't a string
        response = self.client.post(
            "/v0/features/values/parts/entities/counts",
            json=dict(items=[dict(part="port", value=443)]),
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(0, resp["data"]["443"]["port"]["entities"])

        self.flush()
        response = self.client.post(
            "/v0/features/values/parts/entities/counts?skip_count=true",
            json=dict(
                items=[
                    dict(part="hostname", value="blah.com"),  # cached
                    dict(part="hostname", value="ampersand.com"),  # not cached
                ]
            ),
        )
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(
            resp["data"], {"blah.com": {"hostname": {"value": "blah.com", "part": "hostname", "entities": 3}}}
        )

    def test_feature_value_tags_create_delete(self):
        response = self.client.post("/v0/features/tags/1?feature=1&value=1", json=dict(security="low"))
        self.assertEqual(200, response.status_code)

        response = self.client.get("/v0/features/all/tags")
        self.assertEqual(200, response.status_code)
        all_tags = response.json()["data"]["tags"]
        print(f"Actual tags for all tags is {all_tags}")
        self.assertEqual(all_tags, [{"tag": "1", "num_feature_values": 1}])

        response = self.client.delete("/v0/features/tags/1?feature=1&value=1")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(None, resp)

        response = self.client.post("/v0/features/tags/hello.world?feature=1&value=1", json=dict(security="low"))
        self.assertEqual(400, response.status_code)

        response = self.client.post(
            "/v0/features/tags/helloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworld?feature=1&value=1",
            json=dict(security="low"),
        )
        self.assertEqual(400, response.status_code)

        # check case sensitive
        response = self.client.post("/v0/features/tags/tag1?feature=abc1&value=abc1", json=dict(security="low"))
        self.assertEqual(200, response.status_code)
        response = self.client.post("/v0/features/tags/tag1?feature=ABC1&value=ABC1", json=dict(security="low"))
        self.assertEqual(200, response.status_code)
        self.flush()
        response = self.client.get("/v0/features/tags/tag1")
        self.assertEqual(2, len(response.json()["data"]["items"]))

    def test_plugin(self):
        response = self.client.get("/v0/features")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(0, len(resp["data"]["items"]))

        self.write_plugin_events(
            [
                gen.plugin(authornv=("a1", "1"), features=["f1"]),
                gen.plugin(authornv=("a2", "1"), features=["f1"]),
                gen.plugin(authornv=("a3", "1"), features=["f2", "f3", "f4", "f5", "f6"]),
                gen.plugin(authornv=("a3", "2"), features=["f2", "f3", "f4", "f5", "f6", "f7"]),
            ]
        )
        response = self.client.get("/v0/features")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(7, len(resp["data"]["items"]))
        self.assertEqual("f1", resp["data"]["items"][0]["name"])

        response = self.client.get("/v0/features?author=a1")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, len(resp["data"]["items"]))
        self.assertEqual("f1", resp["data"]["items"][0]["name"])

        response = self.client.get("/v0/features?author=a3")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(6, len(resp["data"]["items"]))
        self.assertEqual("f2", resp["data"]["items"][0]["name"])

        response = self.client.get("/v0/features?author=a3&author_version=2")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(6, len(resp["data"]["items"]))
        self.assertEqual("f2", resp["data"]["items"][0]["name"])

    def test_plugin_query_logs(self):
        self.write_plugin_events(
            [
                gen.plugin(authornv=("a1", "1"), features=["f1"]),
                gen.plugin(authornv=("a2", "1"), features=["f1"]),
                gen.plugin(authornv=("a3", "1"), features=["f2", "f3", "f4", "f5", "f6"]),
                gen.plugin(authornv=("a3", "2"), features=["f2", "f3", "f4", "f5", "f6", "f7"]),
            ]
        )
        response = self.client.get("/v0/features?include_queries=true")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, len(resp["meta"]["queries"]))

    def test_plugin_values(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a1", "1"), fvl=[("f1", "v1")]),
                gen.binary_event(eid="e2", authornv=("a1", "2"), fvl=[("f1", "v3")]),
                gen.binary_event(eid="e3", authornv=("a2", "1"), fvl=[("f1", "v1"), ("f1", "v2")]),
            ]
        )

        response = self.client.post("/v0/features/feature/f1")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual("f1", resp["data"]["name"])
        self.assertEqual(3, len(resp["data"]["values"]))

        response = self.client.post("/v0/features/feature/f1?author=a1")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual("f1", resp["data"]["name"])
        self.assertEqual(2, len(resp["data"]["values"]))

        response = self.client.post("/v0/features/feature/f1?author=a1&author_version=2")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual("f1", resp["data"]["name"])
        self.assertEqual(1, len(resp["data"]["values"]))

    def test_sort_and_paginate(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    fvl=[("f1", "v1")],
                    sourceit=("s1", "2021-01-01T12:00+00:00"),
                    timestamp="2020-01-01T12:00+00:00",
                ),
                gen.binary_event(
                    eid="e2",
                    fvl=[("f1", "v2")],
                    sourceit=("s1", "2022-01-01T12:00+00:00"),
                    timestamp="2022-01-01T12:00+00:00",
                ),
                gen.binary_event(
                    eid="e3",
                    fvl=[("f1", "v3")],
                    sourceit=("s1", "2020-01-01T12:00+00:00"),
                    timestamp="2021-01-01T12:00+00:00",
                ),
                # for first_source_timestamp
                gen.binary_event(
                    eid="e1",
                    fvl=[("f1", "v1")],
                    sourceit=("s2", "2000-01-01T12:00+00:00"),
                    timestamp="2020-01-01T12:00+00:00",
                ),
                gen.binary_event(
                    eid="e2",
                    fvl=[("f1", "v2")],
                    sourceit=("s2", "2002-01-01T12:00+00:00"),
                    timestamp="2022-01-01T12:00+00:00",
                ),
                gen.binary_event(
                    eid="e3",
                    fvl=[("f1", "v3")],
                    sourceit=("s2", "2001-01-01T12:00+00:00"),
                    timestamp="2021-01-01T12:00+00:00",
                ),
            ]
        )

        # e3,e2,e1
        response = self.client.post("/v0/features/feature/f1?sort_asc=false")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual("v3", resp["data"]["values"][0]["value"])
        self.assertEqual("v2", resp["data"]["values"][1]["value"])
        self.assertEqual("v1", resp["data"]["values"][2]["value"])

        # e1,e2,e3
        response = self.client.post("/v0/features/feature/f1?sort_asc=true")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual("v1", resp["data"]["values"][0]["value"])
        self.assertEqual("v2", resp["data"]["values"][1]["value"])
        self.assertEqual("v3", resp["data"]["values"][2]["value"])

        # e1,e2,e3 with after queries
        response = self.client.post("/v0/features/feature/f1?num_values=1")
        self.assertEqual(200, response.status_code)
        resp_model = bedr_features.ReadFeatureValues.model_validate(response.json()["data"])
        self.assertEqual('{"VALUE": "v1"}', resp_model.after)
        self.assertFalse(resp_model.is_search_complete)
        self.assertEqual(1, len(resp_model.values))
        self.assertEqual(resp_model.values[0].value, "v1")
        self.assertEqual(resp_model.total, 3)

        # e1,e2,e3 with after queries
        response = self.client.post(f"/v0/features/feature/f1?num_values=1", json={"after": resp_model.after})
        self.assertEqual(200, response.status_code)
        resp_model = bedr_features.ReadFeatureValues.model_validate(response.json()["data"])
        self.assertEqual('{"VALUE": "v2"}', resp_model.after)
        self.assertFalse(resp_model.is_search_complete)
        self.assertEqual(1, len(resp_model.values))
        self.assertEqual(resp_model.values[0].value, "v2")

        # e1,e2,e3 with after queries
        response = self.client.post(f"/v0/features/feature/f1?num_values=2", json={"after": resp_model.after})
        self.assertEqual(200, response.status_code)
        resp_model = bedr_features.ReadFeatureValues.model_validate(response.json()["data"])
        self.assertEqual(None, resp_model.after)
        self.assertTrue(resp_model.is_search_complete)
        self.assertEqual(1, len(resp_model.values))
        self.assertEqual(resp_model.values[0].value, "v3")

    def test_term_query(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    fvl=[("f1", "v1")],
                    sourceit=("s1", "2021-01-01T12:00+00:00"),
                    timestamp="2020-01-01T12:00+00:00",
                ),
                gen.binary_event(
                    eid="e2",
                    fvl=[("f1", "v2")],
                    sourceit=("s1", "2022-01-01T12:00+00:00"),
                    timestamp="2022-01-01T12:00+00:00",
                ),
                gen.binary_event(
                    eid="e3",
                    fvl=[("f1", "v3")],
                    sourceit=("s1", "2020-01-01T12:00+00:00"),
                    timestamp="2021-01-01T12:00+00:00",
                ),
                gen.binary_event(
                    eid="e4",
                    fvl=[("f1", "ddv1")],
                    sourceit=("s1", "2021-01-01T12:00+00:00"),
                    timestamp="2020-01-01T12:00+00:00",
                ),
                gen.binary_event(
                    eid="e5",
                    fvl=[("f1", "ddv2")],
                    sourceit=("s1", "2022-01-01T12:00+00:00"),
                    timestamp="2022-01-01T12:00+00:00",
                ),
                gen.binary_event(
                    eid="e6",
                    fvl=[("f1", "ddv3")],
                    sourceit=("s1", "2020-01-01T12:00+00:00"),
                    timestamp="2021-01-01T12:00+00:00",
                ),
                gen.binary_event(
                    eid="e7",
                    fvl=[("f1", "zzv1")],
                    sourceit=("s1", "2021-01-01T12:00+00:00"),
                    timestamp="2020-01-01T12:00+00:00",
                ),
                gen.binary_event(
                    eid="e8",
                    fvl=[("f1", "zzv2")],
                    sourceit=("s1", "2022-01-01T12:00+00:00"),
                    timestamp="2022-01-01T12:00+00:00",
                ),
                gen.binary_event(
                    eid="e9",
                    fvl=[("f1", "zzv3")],
                    sourceit=("s1", "2020-01-01T12:00+00:00"),
                    timestamp="2021-01-01T12:00+00:00",
                ),
                gen.binary_event(
                    eid="e10",
                    fvl=[
                        ("f1", "1"),
                        ("f1", "2yy"),
                        ("f1", "2yyy"),
                        ("f1", "2yyyy"),
                        ("f1", "2yyyyy"),
                        ("f1", "2yya"),
                        ("f1", "2yyya"),
                        ("f1", "2yyyya"),
                        ("f1", "2yyyyya"),
                        ("f1", "2yyb"),
                        ("f1", "2yyyb"),
                        ("f1", "2yyyyb"),
                        ("f1", "2yyyyyb"),
                        ("f1", "2yyc"),
                        ("f1", "2yyyc"),
                        ("f1", "2yyyyc"),
                        ("f1", "2yyyyyc"),
                    ],
                    sourceit=("s1", "2020-01-01T12:00+00:00"),
                    timestamp="2021-01-01T12:00+00:00",
                ),
                gen.binary_event(
                    eid="e10",
                    fvl=[("f1", "11")],
                    sourceit=("s1", "2020-01-01T12:00+00:00"),
                    timestamp="2021-01-01T12:00+00:00",
                ),
                # Lots of values in one event as this can cause issues when filtering.
                gen.binary_event(
                    eid="e10",
                    fvl=[
                        ("f1", "111"),
                        ("f1", "2zz"),
                        ("f1", "2zzz"),
                        ("f1", "2zzzz"),
                        ("f1", "2zzzzz"),
                        ("f1", "2zza"),
                        ("f1", "2zzza"),
                        ("f1", "2zzzza"),
                        ("f1", "2zzzzza"),
                        ("f1", "2zzb"),
                        ("f1", "2zzzb"),
                        ("f1", "2zzzzb"),
                        ("f1", "2zzzzzb"),
                        ("f1", "2zzc"),
                        ("f1", "2zzzc"),
                        ("f1", "2zzzzc"),
                        ("f1", "2zzzzzc"),
                    ],
                    sourceit=("s1", "2020-01-01T12:00+00:00"),
                    timestamp="2021-01-01T12:00+00:00",
                ),
            ]
        )

        # Test that when the term filter limits it to 3 and the num_values is greater than the expected values everything works.
        response = self.client.post("/v0/features/feature/f1?term=v2&num_values=4")
        self.assertEqual(200, response.status_code)
        print(response.json())
        resp_model = bedr_features.ReadFeatureValues.model_validate(response.json()["data"])
        self.assertEqual(None, resp_model.after)
        self.assertTrue(resp_model.is_search_complete)
        self.assertEqual(3, len(resp_model.values))
        self.assertEqual(resp_model.values[0].value, "ddv2")
        self.assertEqual(resp_model.total, 3)

        # Test if the term results in more values then num_values everything still works and the total is correct
        response = self.client.post("/v0/features/feature/f1?term=v&num_values=4")
        self.assertEqual(200, response.status_code)
        print(response.json())
        resp_model = bedr_features.ReadFeatureValues.model_validate(response.json()["data"])
        self.assertEqual('{"VALUE": "v1"}', resp_model.after)
        self.assertFalse(resp_model.is_search_complete)
        self.assertEqual(4, len(resp_model.values))
        self.assertEqual(resp_model.values[0].value, "ddv1")
        self.assertEqual(resp_model.total, 9)

        # Test if the term query is filtering out some more values and num_values is more then term
        response = self.client.post("/v0/features/feature/f1?term=1&num_values=4")
        self.assertEqual(200, response.status_code)
        print(response.json())
        resp_model = bedr_features.ReadFeatureValues.model_validate(response.json()["data"])
        self.assertEqual('{"VALUE": "zzv1"}', resp_model.after)
        self.assertFalse(resp_model.is_search_complete)
        self.assertEqual(4, len(resp_model.values))
        self.assertEqual(resp_model.values[0].value, "1")
        self.assertEqual(resp_model.total, 4)
        self.assertTrue(resp_model.is_total_approx)  # TODO? - REALLY!

        # Second query that will get no value because precisely 4 matched.
        response = self.client.post(f"/v0/features/feature/f1?term=1&num_values=4", json={"after": resp_model.after})
        self.assertEqual(200, response.status_code)
        print(response.json())
        resp_model = bedr_features.ReadFeatureValues.model_validate(response.json()["data"])
        self.assertEqual(None, resp_model.after)
        self.assertTrue(resp_model.is_search_complete)
        self.assertEqual(0, len(resp_model.values))

        # Lower num_values a bit more and r-run to see the different behaviour ()
        response = self.client.post("/v0/features/feature/f1?term=1&num_values=3")
        self.assertEqual(200, response.status_code)
        print(response.json())
        resp_model = bedr_features.ReadFeatureValues.model_validate(response.json()["data"])
        self.assertEqual('{"VALUE": "v1"}', resp_model.after)
        self.assertFalse(resp_model.is_search_complete)
        self.assertEqual(3, len(resp_model.values))
        self.assertEqual(resp_model.values[0].value, "1")
        self.assertEqual(resp_model.total, 3)
        self.assertTrue(resp_model.is_total_approx)

        # Second query that will get one value because there was one leftover from the last query.
        response = self.client.post(f"/v0/features/feature/f1?term=1&num_values=3", json={"after": resp_model.after})
        self.assertEqual(200, response.status_code)
        print(response.json())
        resp_model = bedr_features.ReadFeatureValues.model_validate(response.json()["data"])
        self.assertEqual(None, resp_model.after)
        self.assertTrue(resp_model.is_search_complete)
        self.assertEqual(1, len(resp_model.values))
