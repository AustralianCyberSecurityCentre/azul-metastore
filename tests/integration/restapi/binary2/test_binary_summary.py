import copy
import datetime

from azul_bedrock import models_network as azm

from tests.support import gen, integration_test


class TestBinary(integration_test.BaseRestapi):
    def test_streams(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", datas=[gen.data(hash="e1"), gen.data({"label": azm.DataLabel.TEST})]),
                gen.binary_event(eid="e2", datas=[gen.data(hash="e3")]),
                gen.binary_event(eid="e2", authornv=("p1", "1"), datas=[gen.data({"label": azm.DataLabel.TEST})]),
            ],
        )

        response = self.client.get("/v0/binaries/e1")
        self.assertEqual(200, response.status_code)
        self.assertFormatted(
            response.json()["data"]["streams"],
            [
                {
                    "md5": "000000000000000000000000000000e1",
                    "sha1": "00000000000000000000000000000000000000e1",
                    "sha256": "e1",
                    "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e1",
                    "ssdeep": "1:1:1",
                    "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                    "size": 1024,
                    "file_format_legacy": "Text",
                    "file_format": "text/plain",
                    "file_extension": "txt",
                    "mime": "text/plain",
                    "magic": "ASCII text",
                    "identify_version": 1,
                    "label": ["content"],
                    "instances": ["e1.plugin.generic_plugin.sourced"],
                },
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
                    "label": ["test"],
                    "instances": ["e1.plugin.generic_plugin.sourced"],
                },
            ],
        )
        response = self.client.get("/v0/binaries/e2")
        self.assertEqual(200, response.status_code)
        self.assertFormatted(
            response.json()["data"]["streams"],
            [
                {
                    "md5": "000000000000000000000000000000e3",
                    "sha1": "00000000000000000000000000000000000000e3",
                    "sha256": "e3",
                    "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e3",
                    "ssdeep": "1:1:1",
                    "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                    "size": 1024,
                    "file_format_legacy": "Text",
                    "file_format": "text/plain",
                    "file_extension": "txt",
                    "mime": "text/plain",
                    "magic": "ASCII text",
                    "identify_version": 1,
                    "label": ["content"],
                    "instances": ["e2.plugin.generic_plugin.sourced"],
                },
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
                    "label": ["test"],
                    "instances": ["e2.plugin.p1.sourced"],
                },
            ],
        )

        # should get a 404 since we had bad submission - no label
        response = self.client.get("/v0/binaries/e3")
        self.assertEqual(404, response.status_code)

    def test_feature_labels(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", features=[gen.feature({"label": "serendipitous"}, fv=("f", "1"))]),
                gen.binary_event(
                    eid="e2",
                    features=[
                        gen.feature({"label": "serendipitous"}, fv=("f", "1")),
                        gen.feature({"label": ""}, fv=("f", "1")),
                    ],
                ),
                gen.binary_event(
                    eid="e3",
                    features=[
                        gen.feature({"label": "serendipitous"}, fv=("f", "1")),
                        gen.feature({"label": ""}, fv=("f", "2")),
                    ],
                ),
            ]
        )
        response = self.client.get("/v0/binaries/e1")
        self.assertEqual(200, response.status_code)
        self.assertFormatted(
            response.json()["data"]["features"],
            [
                {
                    "name": "f",
                    "value": "1",
                    "type": "string",
                    "label": ["serendipitous"],
                    "parts": {},
                    "instances": ["e1.plugin.generic_plugin.sourced"],
                }
            ],
        )

        response = self.client.get("/v0/binaries/e2")
        self.assertEqual(200, response.status_code)
        self.assertFormatted(
            response.json()["data"]["features"],
            [
                {
                    "name": "f",
                    "value": "1",
                    "type": "string",
                    "label": ["serendipitous"],
                    "parts": {},
                    "instances": ["e2.plugin.generic_plugin.sourced"],
                }
            ],
        )

        response = self.client.get("/v0/binaries/e3")
        self.assertEqual(200, response.status_code)
        self.assertFormatted(
            response.json()["data"]["features"],
            [
                {
                    "name": "f",
                    "value": "1",
                    "type": "string",
                    "label": ["serendipitous"],
                    "parts": {},
                    "instances": ["e3.plugin.generic_plugin.sourced"],
                },
                {
                    "name": "f",
                    "value": "2",
                    "type": "string",
                    "label": [],
                    "parts": {},
                    "instances": ["e3.plugin.generic_plugin.sourced"],
                },
            ],
        )

    def test_basic(self):
        expected = {
            "data": {
                "documents": {"count": 1, "newest": "2021-01-01T12:00:00.000Z"},
                "security": ["LOW TLP:CLEAR"],
                "sources": [
                    {
                        "source": "generic_source",
                        "direct": [
                            {
                                "security": "LOW TLP:CLEAR",
                                "name": "generic_source",
                                "timestamp": "2021-01-01T11:00:00Z",
                                "references": {"ref2": "val2", "ref1": "val1"},
                                "track_source_references": "generic_source.dd6e233ae7a843de99f9b43c349069e4",
                            }
                        ],
                        "indirect": [],
                    }
                ],
                "parents": [],
                "children": [],
                "instances": [
                    {
                        "key": "e1.plugin.generic_plugin.sourced",
                        "author": {
                            "security": "LOW TLP:CLEAR",
                            "category": "plugin",
                            "name": "generic_plugin",
                            "version": "2021-01-01T12:00:00+00:00",
                        },
                        "action": "sourced",
                        "num_feature_values": 1,
                    }
                ],
                "features": [
                    {
                        "name": "generic_feature",
                        "type": "string",
                        "value": "generic_value",
                        "label": [],
                        "parts": {},
                        "instances": ["e1.plugin.generic_plugin.sourced"],
                    }
                ],
                "streams": [
                    {
                        "sha256": "e1",
                        "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e1",
                        "sha1": "00000000000000000000000000000000000000e1",
                        "md5": "000000000000000000000000000000e1",
                        "ssdeep": "1:1:1",
                        "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                        "size": 1024,
                        "file_format_legacy": "Text",
                        "file_format": "text/plain",
                        "file_extension": "txt",
                        "mime": "text/plain",
                        "magic": "ASCII text",
                        "identify_version": 1,
                        "label": ["content"],
                        "instances": ["e1.plugin.generic_plugin.sourced"],
                    }
                ],
                "info": [],
                "tags": [],
            },
            "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
        }
        self.write_binary_events(
            [
                gen.binary_event(eid="e1"),
            ]
        )
        response = self.client.get("/v0/binaries/e1")
        self.assertEqual(200, response.status_code)
        self.assertFormatted(response.json(), expected)

        response = self.client.get("/v0/binaries/E1")
        self.assertEqual(200, response.status_code)
        self.assertFormatted(response.json(), expected)

        response = self.client.get("/v0/binaries/E1?features_size=1")
        self.assertEqual(200, response.status_code)
        self.assertFormatted(response.json(), expected)

        response = self.client.get("/v0/binaries/invalid1")
        self.assertEqual(404, response.status_code)

    def test_basic_various_feature_types(self):
        expected = {
            "data": {
                "documents": {"count": 1, "newest": "2021-01-01T12:00:00.000Z"},
                "security": ["LOW TLP:CLEAR"],
                "sources": [
                    {
                        "source": "generic_source",
                        "direct": [
                            {
                                "security": "LOW TLP:CLEAR",
                                "name": "generic_source",
                                "timestamp": "2021-01-01T11:00:00Z",
                                "references": {"ref2": "val2", "ref1": "val1"},
                                "track_source_references": "generic_source.dd6e233ae7a843de99f9b43c349069e4",
                            }
                        ],
                        "indirect": [],
                    }
                ],
                "parents": [],
                "children": [],
                "instances": [
                    {
                        "key": "e1.plugin.generic_plugin.sourced",
                        "author": {
                            "security": "LOW TLP:CLEAR",
                            "category": "plugin",
                            "name": "generic_plugin",
                            "version": "2021-01-01T12:00:00+00:00",
                        },
                        "action": "sourced",
                        "num_feature_values": 6,
                    }
                ],
                "features": [
                    {
                        "name": "generic_feature",
                        "type": "string",
                        "value": "generic_value",
                        "label": [],
                        "parts": {},
                        "instances": ["e1.plugin.generic_plugin.sourced"],
                    },
                    {
                        "name": "generic_feature_bad_string",
                        "type": "string",
                        "value": "1.123",
                        "label": [],
                        "parts": {},
                        "instances": ["e1.plugin.generic_plugin.sourced"],
                    },
                    {
                        "name": "generic_feature_binary",
                        "type": "binary",
                        "value": "generic_byte_value",
                        "label": [],
                        "parts": {"binary_string": "ޮ'\x1bׯj["},
                        "instances": ["e1.plugin.generic_plugin.sourced"],
                    },
                    {
                        "name": "generic_feature_date",
                        "type": "datetime",
                        "value": "2000-01-01T01:01:00",
                        "label": [],
                        "parts": {"datetime": "2000-01-01T01:01:00"},
                        "instances": ["e1.plugin.generic_plugin.sourced"],
                    },
                    {
                        "name": "generic_feature_float",
                        "type": "float",
                        "value": "1.123",
                        "label": [],
                        "parts": {"float": "1.123"},
                        "instances": ["e1.plugin.generic_plugin.sourced"],
                    },
                    {
                        "name": "generic_feature_int",
                        "type": "integer",
                        "value": "123",
                        "label": [],
                        "parts": {"integer": "123"},
                        "instances": ["e1.plugin.generic_plugin.sourced"],
                    },
                ],
                "streams": [
                    {
                        "sha256": "e1",
                        "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e1",
                        "sha1": "00000000000000000000000000000000000000e1",
                        "md5": "000000000000000000000000000000e1",
                        "ssdeep": "1:1:1",
                        "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                        "size": 1024,
                        "file_format_legacy": "Text",
                        "file_format": "text/plain",
                        "file_extension": "txt",
                        "mime": "text/plain",
                        "magic": "ASCII text",
                        "identify_version": 1,
                        "label": ["content"],
                        "instances": ["e1.plugin.generic_plugin.sourced"],
                    }
                ],
                "info": [],
                "tags": [],
            },
            "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
        }
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    features=[
                        {
                            "name": "generic_feature",
                            "value": "generic_value",
                            "type": "string",
                        },
                        {  # FUTURE this shouldn't be required.
                            "name": "generic_feature_bad_string",
                            "value": "1.123",
                            "type": "string",
                        },
                        {
                            "name": "generic_feature_int",
                            "value": "123",
                            "type": "integer",
                        },
                        {
                            "name": "generic_feature_float",
                            "value": "1.123",
                            "type": "float",
                        },
                        {
                            "name": "generic_feature_date",
                            "value": datetime.datetime(2000, 1, 1, 1, 1, 0, 0).isoformat(),
                            "type": "datetime",
                        },
                        {
                            "name": "generic_feature_binary",
                            "value": "generic_byte_value",
                            "type": "binary",
                        },
                    ],
                ),
            ]
        )
        response = self.client.get("/v0/binaries/e1")
        self.assertEqual(200, response.status_code)
        self.assertFormatted(response.json(), expected)

    def test_binary_read_descendants(self):
        a = ("a1", "1")
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", spathl=[]),
                gen.binary_event(eid="e10", spathl=[("e1", a)]),
                gen.binary_event(eid="e100", spathl=[("e10", a)]),
                gen.binary_event(eid="e2", spathl=[]),
            ]
        )
        response = self.client.get("/v0/binaries/e1")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(
            resp["data"]["children"],
            [
                {
                    "sha256": "e10",
                    "action": "extracted",
                    "timestamp": "2021-01-01T12:00:00Z",
                    "author": {
                        "category": "plugin",
                        "name": "generic_plugin",
                        "version": "2021-01-01T12:00:00+00:00",
                        "security": "LOW TLP:CLEAR",
                    },
                    "relationship": {"random": "data", "action": "extracted", "label": "within"},
                    "file_format_legacy": "Text",
                    "file_format": "text/plain",
                    "size": 1024,
                    "track_link": "e1.e10.plugin.generic_plugin.2021-01-01T12:00:00+00:00",
                }
            ],
        )

        response = self.client.get("/v0/binaries/E1")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(
            resp["data"]["children"],
            [
                {
                    "author": {
                        "security": "LOW TLP:CLEAR",
                        "category": "plugin",
                        "name": "generic_plugin",
                        "version": "2021-01-01T12:00:00+00:00",
                    },
                    "action": "extracted",
                    "timestamp": "2021-01-01T12:00:00Z",
                    "relationship": {"random": "data", "action": "extracted", "label": "within"},
                    "sha256": "e10",
                    "size": 1024,
                    "file_format_legacy": "Text",
                    "file_format": "text/plain",
                    "track_link": "e1.e10.plugin.generic_plugin.2021-01-01T12:00:00+00:00",
                }
            ],
        )

    def test_buckets(self):
        # write 20 events that would require 20 buckets, but when randomly selected would result in
        # same total number of each thing they contain (as we don't know which will be selected to bucket)
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="ebucket1",
                    sourceit=("s1", f"2000-01-{x+1:02d}T01:02:01Z"),
                    authornv=(f"a{x}", "1"),
                    fvl=[(f"f{x}", f"v{x}")],
                    info={f"ssdeep{x}": None},
                    datas=[gen.data({"label": azm.DataLabel.TEST}, hash=f"a{x}")],
                )
                for x in range(20)
            ]
            + [
                gen.binary_event(
                    eid=f"echild{x}",
                    sourceit=("s1", f"2000-01-{x+1:02d}T01:02:01Z"),
                    authornv=("author1", "1"),
                    spathl=[("ebucket1", ("a1", "1"))],
                )
                for x in range(20)
            ]
        )
        response = self.client.get("/v0/binaries/ebucket1")
        self.assertEqual(200, response.status_code)
        self.assertEqual(len(response.json()["data"]["features"]), 20)
        self.assertEqual(len(response.json()["data"]["info"]), 20)
        # FUTURE edit available sources so we can do f"s{x}"
        self.assertEqual(len(response.json()["data"]["sources"]), 1)
        self.assertEqual(len(response.json()["data"]["instances"]), 20)
        self.assertEqual(len(response.json()["data"]["streams"]), 20)
        self.assertEqual(len(response.json()["data"]["children"]), 20)

        # BUCKET_SIZE 1
        response = self.client.get("/v0/binaries/ebucket1?bucket_size=1")
        warnings = response.json()["data"]["diagnostics"]
        # Check warnings
        self.assertFormatted(
            warnings,
            [
                {
                    "severity": "warning",
                    "id": "many_buckets_datastreams",
                    "title": "Results may be missing for datastreams",
                    "body": "There were more fields identified by the database (over 1). Consider increasing query size in user settings.",
                },
                {
                    "severity": "warning",
                    "id": "many_buckets_features",
                    "title": "Results may be missing for features",
                    "body": "There were more fields identified by the database (over 1). Consider increasing query size in user settings.",
                },
                {
                    "severity": "warning",
                    "id": "many_buckets_info",
                    "title": "Results may be missing for info",
                    "body": "There were more fields identified by the database (over 1). Consider increasing query size in user settings.",
                },
                {
                    "severity": "warning",
                    "id": "many_buckets_instances",
                    "title": "Results may be missing for instances",
                    "body": "There were more fields identified by the database (over 1). Consider increasing query size in user settings.",
                },
                {
                    "severity": "warning",
                    "id": "many_buckets_security",
                    "title": "Results may be missing for security",
                    "body": "There were more fields identified by the database (over 1). Consider increasing query size in user settings.",
                },
                {
                    "severity": "warning",
                    "id": "many_buckets_sources",
                    "title": "Results may be missing for sources",
                    "body": "There were more fields identified by the database (over 1). Consider increasing query size in user settings.",
                },
                {
                    "severity": "warning",
                    "id": "many_buckets_sources_s1_references",
                    "title": "Results may be missing for sources_s1_references",
                    "body": "There were more fields identified by the database (over 1). Consider increasing query size in user settings.",
                },
                {
                    "severity": "warning",
                    "id": "many_events",
                    "title": "Many events",
                    "body": "This binary has many events in the system (20) and accurate results can't be guaranteed.",
                },
                {
                    "severity": "info",
                    "id": "no_content",
                    "title": "Content not found",
                    "body": "The content for this binary is not in Azul and as such this file is unlikely to be processed by most plugins.",
                },
                {
                    "severity": "warning",
                    "id": "many_buckets_children",
                    "title": "Results may be missing for children",
                    "body": "There were more fields identified by the database (over 1). Consider increasing query size in user settings.",
                },
            ],
        )
        # Check data
        self.assertEqual(len(response.json()["data"]["features"]), 1)
        self.assertEqual(len(response.json()["data"]["info"]), 1)
        self.assertEqual(len(response.json()["data"]["instances"]), 1)
        self.assertEqual(len(response.json()["data"]["streams"]), 1)

        # BUCKET_SIZE 4
        response = self.client.get("/v0/binaries/ebucket1?bucket_size=4")
        warnings = response.json()["data"]["diagnostics"]

        # Check Warnings
        self.assertFormatted(
            warnings,
            [
                {
                    "severity": "warning",
                    "id": "many_buckets_datastreams",
                    "title": "Results may be missing for datastreams",
                    "body": "There were more fields identified by the database (over 4). Consider increasing query size in user settings.",
                },
                {
                    "severity": "warning",
                    "id": "many_buckets_features",
                    "title": "Results may be missing for features",
                    "body": "There were more fields identified by the database (over 4). Consider increasing query size in user settings.",
                },
                {
                    "severity": "warning",
                    "id": "many_buckets_info",
                    "title": "Results may be missing for info",
                    "body": "There were more fields identified by the database (over 4). Consider increasing query size in user settings.",
                },
                {
                    "severity": "warning",
                    "id": "many_buckets_instances",
                    "title": "Results may be missing for instances",
                    "body": "There were more fields identified by the database (over 4). Consider increasing query size in user settings.",
                },
                {
                    "severity": "warning",
                    "id": "many_events",
                    "title": "Many events",
                    "body": "This binary has many events in the system (20) and accurate results can't be guaranteed.",
                },
                {
                    "severity": "info",
                    "id": "no_content",
                    "title": "Content not found",
                    "body": "The content for this binary is not in Azul and as such this file is unlikely to be processed by most plugins.",
                },
                {
                    "severity": "warning",
                    "id": "many_buckets_children",
                    "title": "Results may be missing for children",
                    "body": "There were more fields identified by the database (over 4). Consider increasing query size in user settings.",
                },
            ],
        )
        # Check data length
        print(response.json()["data"]["features"])
        self.assertEqual(len(response.json()["data"]["features"]), 4)
        self.assertEqual(len(response.json()["data"]["info"]), 4)
        self.assertEqual(len(response.json()["data"]["instances"]), 4)
        self.assertEqual(len(response.json()["data"]["streams"]), 4)

    def test_include_queries(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="equery", authornv=("a1", "1"), fvl=[("f1", f"v{x}") for x in range(10)]),
                gen.binary_event(eid="equery", fvl=[("f1", "v2")]),
                gen.binary_event(eid="equery", sourceit=("s2", "2000-01-01T01:01:01Z"), fvl=[("f1", "v3")]),
                gen.binary_event(eid="equery", sourceit=("s1", "2000-01-01T01:01:01Z"), fvl=[("f1", "v4")]),
                gen.binary_event(eid="equery", authornv=("a1", "1"), fvl=[("f1", "v5")]),
                gen.binary_event(eid="equery", authornv=("a1", "1"), info={"ssdeep": None}),
                gen.binary_event(eid="equery", authornv=("a2", "2"), info={"other": None}),
                gen.binary_event(
                    eid="equery",
                    authornv=("a3", "3"),
                    info={"xyz": None},
                    datas=[gen.data({"label": azm.DataLabel.TEST})],
                ),
            ]
        )
        # This is done to generate more than one stream which will be returned by Opensearch as a set which can cause
        # serialization issues. (json.dumps can't serialize sets.)
        self.write_binary_events(
            [
                gen.binary_event(eid="equery", datas=[gen.data(hash="e1"), gen.data({"label": azm.DataLabel.TEST})]),
                gen.binary_event(eid="equery", datas=[gen.data(hash="e3")]),
                gen.binary_event(eid="equery", authornv=("p1", "1"), datas=[gen.data({"label": azm.DataLabel.TEST})]),
            ],
        )
        response = self.client.get("/v0/binaries/equery?include_queries=true")
        self.assertEqual(response.status_code, 200)
        queries = response.json()["meta"]["queries"]
        self.assertGreater(len(queries), 3)

    def test_children_parents(self):
        a = ("p1", "1")
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", spathl=[]),
                gen.binary_event(eid="e10", spathl=[("e1", a)]),
                gen.binary_event(eid="e100", spathl=[("e1", a), ("e10", a)]),
            ]
        )

        response = self.client.get("/v0/binaries/e10")
        self.assertEqual(200, response.status_code)
        self.assertFormatted(
            response.json(),
            {
                "data": {
                    "documents": {"count": 1, "newest": "2021-01-01T12:00:00.000Z"},
                    "security": ["LOW TLP:CLEAR"],
                    "sources": [
                        {
                            "source": "generic_source",
                            "direct": [],
                            "indirect": [
                                {
                                    "security": "LOW TLP:CLEAR",
                                    "name": "generic_source",
                                    "timestamp": "2021-01-01T11:00:00Z",
                                    "references": {"ref2": "val2", "ref1": "val1"},
                                    "track_source_references": "generic_source.dd6e233ae7a843de99f9b43c349069e4",
                                }
                            ],
                        }
                    ],
                    "parents": [
                        {
                            "sha256": "e1",
                            "action": "extracted",
                            "timestamp": "2021-01-01T12:00:00Z",
                            "author": {
                                "category": "plugin",
                                "name": "generic_plugin",
                                "version": "2021-01-01T12:00:00+00:00",
                                "security": "LOW TLP:CLEAR",
                            },
                            "relationship": {"random": "data", "action": "extracted", "label": "within"},
                            "file_format_legacy": "Text",
                            "file_format": "text/plain",
                            "size": 1024,
                            "track_link": "e1.e10.plugin.generic_plugin.2021-01-01T12:00:00+00:00",
                        }
                    ],
                    "children": [
                        {
                            "sha256": "e100",
                            "action": "extracted",
                            "timestamp": "2021-01-01T12:00:00Z",
                            "author": {
                                "category": "plugin",
                                "name": "generic_plugin",
                                "version": "2021-01-01T12:00:00+00:00",
                                "security": "LOW TLP:CLEAR",
                            },
                            "relationship": {"random": "data", "action": "extracted", "label": "within"},
                            "file_format_legacy": "Text",
                            "file_format": "text/plain",
                            "size": 1024,
                            "track_link": "e10.e100.plugin.generic_plugin.2021-01-01T12:00:00+00:00",
                        }
                    ],
                    "instances": [
                        {
                            "key": "e10.plugin.generic_plugin.extracted",
                            "author": {
                                "security": "LOW TLP:CLEAR",
                                "category": "plugin",
                                "name": "generic_plugin",
                                "version": "2021-01-01T12:00:00+00:00",
                            },
                            "action": "extracted",
                            "num_feature_values": 1,
                        }
                    ],
                    "features": [
                        {
                            "name": "generic_feature",
                            "type": "string",
                            "value": "generic_value",
                            "label": [],
                            "parts": {},
                            "instances": ["e10.plugin.generic_plugin.extracted"],
                        }
                    ],
                    "streams": [
                        {
                            "sha256": "e10",
                            "sha512": "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e10",
                            "sha1": "0000000000000000000000000000000000000e10",
                            "md5": "00000000000000000000000000000e10",
                            "ssdeep": "1:1:1",
                            "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                            "size": 1024,
                            "file_format_legacy": "Text",
                            "file_format": "text/plain",
                            "file_extension": "txt",
                            "mime": "text/plain",
                            "magic": "ASCII text",
                            "identify_version": 1,
                            "label": ["content"],
                            "instances": ["e10.plugin.generic_plugin.extracted"],
                        }
                    ],
                    "info": [],
                    "tags": [],
                },
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )

    def test_details(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="equery",
                    authornv=("a1", "1"),
                    fvl=[("f1", f"v{x}") for x in range(10)],
                    info={"ssdeep": None},
                    datas=[gen.data(hash="e3")],
                ),
                gen.binary_event(
                    eid="equery",
                    authornv=("a3", "1"),
                    sourceit=("s2", "2000-01-01T01:01:01Z"),
                    fvl=[("f5", "v3")],
                    info={"other": None},
                    spathl=[("eparent", ("me", "1"))],
                    datas=[gen.data({"label": azm.DataLabel.TEST}), gen.data(hash="e3")],
                ),
                gen.binary_event(eid="echild", authornv=("a10", "1"), spathl=[("equery", ("me", "1"))]),
                gen.binary_event(eid="eparent", authornv=("a11", "1")),
            ]
        )
        self.write_fv_tags(
            [
                gen.feature_value_tag(fv=("f1", "v1"), tag="t1"),
            ]
        )
        self.write_entity_tags(
            [
                gen.entity_tag(eid="equery", tag="t1"),
                gen.entity_tag(eid="equery", tag="t2"),
            ]
        )

        # get overall info to compare with results of each item
        response = self.client.get("/v0/binaries/equery")
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {
                    "documents": {"count": 2, "newest": "2021-01-01T12:00:00.000Z"},
                    "security": ["LOW TLP:CLEAR"],
                    "sources": [
                        {
                            "source": "generic_source",
                            "direct": [
                                {
                                    "security": "LOW TLP:CLEAR",
                                    "name": "generic_source",
                                    "timestamp": "2021-01-01T11:00:00Z",
                                    "references": {"ref2": "val2", "ref1": "val1"},
                                    "track_source_references": "generic_source.dd6e233ae7a843de99f9b43c349069e4",
                                }
                            ],
                            "indirect": [],
                        },
                        {
                            "source": "s2",
                            "direct": [],
                            "indirect": [
                                {
                                    "security": "LOW TLP:CLEAR",
                                    "name": "s2",
                                    "timestamp": "2000-01-01T01:01:01Z",
                                    "references": {"ref2": "val2", "ref1": "val1"},
                                    "track_source_references": "s2.dd6e233ae7a843de99f9b43c349069e4",
                                }
                            ],
                        },
                    ],
                    "parents": [
                        {
                            "sha256": "eparent",
                            "action": "extracted",
                            "timestamp": "2021-01-01T12:00:00Z",
                            "author": {
                                "category": "plugin",
                                "name": "a3",
                                "version": "1",
                                "security": "LOW TLP:CLEAR",
                            },
                            "relationship": {"random": "data", "action": "extracted", "label": "within"},
                            "file_format_legacy": "Text",
                            "file_format": "text/plain",
                            "size": 1024,
                            "track_link": "eparent.equery.plugin.a3.1",
                        }
                    ],
                    "children": [
                        {
                            "sha256": "echild",
                            "action": "extracted",
                            "timestamp": "2021-01-01T12:00:00Z",
                            "author": {
                                "category": "plugin",
                                "name": "a10",
                                "version": "1",
                                "security": "LOW TLP:CLEAR",
                            },
                            "relationship": {"random": "data", "action": "extracted", "label": "within"},
                            "file_format_legacy": "Text",
                            "file_format": "text/plain",
                            "size": 1024,
                            "track_link": "equery.echild.plugin.a10.1",
                        }
                    ],
                    "instances": [
                        {
                            "key": "equery.plugin.a1.sourced",
                            "author": {
                                "security": "LOW TLP:CLEAR",
                                "category": "plugin",
                                "name": "a1",
                                "version": "1",
                            },
                            "action": "sourced",
                            "num_feature_values": 10,
                        },
                        {
                            "key": "equery.plugin.a3.extracted",
                            "author": {
                                "security": "LOW TLP:CLEAR",
                                "category": "plugin",
                                "name": "a3",
                                "version": "1",
                            },
                            "action": "extracted",
                            "num_feature_values": 1,
                        },
                    ],
                    "features": [
                        {
                            "name": "f1",
                            "type": "string",
                            "value": "v0",
                            "label": [],
                            "parts": {},
                            "instances": ["equery.plugin.a1.sourced"],
                        },
                        {
                            "name": "f1",
                            "type": "string",
                            "value": "v1",
                            "label": [],
                            "parts": {},
                            "instances": ["equery.plugin.a1.sourced"],
                            "tags": [
                                {
                                    "feature_name": "f1",
                                    "feature_value": "v1",
                                    "tag": "t1",
                                    "type": "fv_tag",
                                    "owner": "generic_owner",
                                    "timestamp": "2000-01-01T01:01:01Z",
                                    "security": "LOW TLP:CLEAR",
                                }
                            ],
                        },
                        {
                            "name": "f1",
                            "type": "string",
                            "value": "v2",
                            "label": [],
                            "parts": {},
                            "instances": ["equery.plugin.a1.sourced"],
                        },
                        {
                            "name": "f1",
                            "type": "string",
                            "value": "v3",
                            "label": [],
                            "parts": {},
                            "instances": ["equery.plugin.a1.sourced"],
                        },
                        {
                            "name": "f1",
                            "type": "string",
                            "value": "v4",
                            "label": [],
                            "parts": {},
                            "instances": ["equery.plugin.a1.sourced"],
                        },
                        {
                            "name": "f1",
                            "type": "string",
                            "value": "v5",
                            "label": [],
                            "parts": {},
                            "instances": ["equery.plugin.a1.sourced"],
                        },
                        {
                            "name": "f1",
                            "type": "string",
                            "value": "v6",
                            "label": [],
                            "parts": {},
                            "instances": ["equery.plugin.a1.sourced"],
                        },
                        {
                            "name": "f1",
                            "type": "string",
                            "value": "v7",
                            "label": [],
                            "parts": {},
                            "instances": ["equery.plugin.a1.sourced"],
                        },
                        {
                            "name": "f1",
                            "type": "string",
                            "value": "v8",
                            "label": [],
                            "parts": {},
                            "instances": ["equery.plugin.a1.sourced"],
                        },
                        {
                            "name": "f1",
                            "type": "string",
                            "value": "v9",
                            "label": [],
                            "parts": {},
                            "instances": ["equery.plugin.a1.sourced"],
                        },
                        {
                            "name": "f5",
                            "type": "string",
                            "value": "v3",
                            "label": [],
                            "parts": {},
                            "instances": ["equery.plugin.a3.extracted"],
                        },
                    ],
                    "streams": [
                        {
                            "sha256": "e3",
                            "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e3",
                            "sha1": "00000000000000000000000000000000000000e3",
                            "md5": "000000000000000000000000000000e3",
                            "ssdeep": "1:1:1",
                            "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                            "size": 1024,
                            "file_format_legacy": "Text",
                            "file_format": "text/plain",
                            "file_extension": "txt",
                            "mime": "text/plain",
                            "magic": "ASCII text",
                            "identify_version": 1,
                            "label": ["content"],
                            "instances": ["equery.plugin.a1.sourced", "equery.plugin.a3.extracted"],
                        },
                        {
                            "sha256": "00000000000000000000000000000000000000000000000000000000000000ab",
                            "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ab",
                            "sha1": "00000000000000000000000000000000000000ab",
                            "md5": "000000000000000000000000000000ab",
                            "ssdeep": "1:1:1",
                            "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                            "size": 1024,
                            "file_format_legacy": "Text",
                            "file_format": "text/plain",
                            "file_extension": "txt",
                            "mime": "text/plain",
                            "magic": "ASCII text",
                            "identify_version": 1,
                            "label": ["test"],
                            "instances": ["equery.plugin.a3.extracted"],
                        },
                    ],
                    "info": [
                        {"info": {"ssdeep": None}, "instance": "equery.plugin.a1.sourced"},
                        {"info": {"other": None}, "instance": "equery.plugin.a3.extracted"},
                    ],
                    "tags": [
                        {
                            "sha256": "equery",
                            "tag": "t1",
                            "type": "entity_tag",
                            "owner": "generic_owner",
                            "timestamp": "2000-01-01T01:01:01Z",
                            "security": "LOW TLP:CLEAR",
                            "num_entities": 1,
                        },
                        {
                            "sha256": "equery",
                            "tag": "t2",
                            "type": "entity_tag",
                            "owner": "generic_owner",
                            "timestamp": "2000-01-01T01:01:01Z",
                            "security": "LOW TLP:CLEAR",
                            "num_entities": 1,
                        },
                    ],
                },
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )
        expected = response.json()["data"]

        response = self.client.get("/v0/binaries/equery?detail=total_hits")
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {"documents": {"count": expected["documents"]["count"]}},
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )

        response = self.client.get("/v0/binaries/equery?detail=documents")
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {"documents": expected["documents"]},
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )

        response = self.client.get("/v0/binaries/equery?detail=security")
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {"documents": {"count": expected["documents"]["count"]}, "security": expected["security"]},
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )

        response = self.client.get("/v0/binaries/equery?detail=sources")
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {"documents": {"count": expected["documents"]["count"]}, "sources": expected["sources"]},
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )

        response = self.client.get("/v0/binaries/equery?detail=features")
        expected_features = copy.deepcopy(expected["features"])
        for x in expected_features:
            x.pop("tags", None)
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {"documents": {"count": expected["documents"]["count"]}, "features": expected_features},
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )

        response = self.client.get("/v0/binaries/equery?detail=info")
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {
                    "documents": {"count": expected["documents"]["count"]},
                    "info": expected["info"],
                },
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )

        response = self.client.get("/v0/binaries/equery?detail=datastreams")
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {"documents": {"count": expected["documents"]["count"]}, "streams": expected["streams"]},
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )

        response = self.client.get("/v0/binaries/equery?detail=instances")
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {"documents": {"count": expected["documents"]["count"]}, "instances": expected["instances"]},
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )

        response = self.client.get("/v0/binaries/equery?detail=parents")
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {"documents": {"count": expected["documents"]["count"]}, "parents": expected["parents"]},
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )

        response = self.client.get("/v0/binaries/equery?detail=children")
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {"documents": {"count": expected["documents"]["count"]}, "children": expected["children"]},
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )

        response = self.client.get("/v0/binaries/equery?detail=tags")
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {"documents": {"count": expected["documents"]["count"]}, "tags": expected["tags"]},
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )

        response = self.client.get("/v0/binaries/equery?detail=feature_tags")
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {"documents": {"count": expected["documents"]["count"]}, "features": expected["features"]},
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )

        # test author filtering
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="equery",
                    authornv=("entropy", "1"),
                    info={"entropy": {"idk": True, "blocks": [1, 2, 8, 4, 5, 6, 7]}},
                ),
            ]
        )

        # test reading for specific author
        response = self.client.get("/v0/binaries/equery?author=entropy")
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {
                    "documents": {"count": 1, "newest": "2021-01-01T12:00:00.000Z"},
                    "security": ["LOW TLP:CLEAR"],
                    "sources": [
                        {
                            "source": "generic_source",
                            "direct": [
                                {
                                    "security": "LOW TLP:CLEAR",
                                    "name": "generic_source",
                                    "timestamp": "2021-01-01T11:00:00Z",
                                    "references": {"ref2": "val2", "ref1": "val1"},
                                    "track_source_references": "generic_source.dd6e233ae7a843de99f9b43c349069e4",
                                }
                            ],
                            "indirect": [],
                        }
                    ],
                    "parents": [],
                    "children": [
                        {
                            "sha256": "echild",
                            "action": "extracted",
                            "timestamp": "2021-01-01T12:00:00Z",
                            "author": {
                                "category": "plugin",
                                "name": "a10",
                                "version": "1",
                                "security": "LOW TLP:CLEAR",
                            },
                            "relationship": {"random": "data", "action": "extracted", "label": "within"},
                            "file_format_legacy": "Text",
                            "file_format": "text/plain",
                            "size": 1024,
                            "track_link": "equery.echild.plugin.a10.1",
                        }
                    ],
                    "instances": [
                        {
                            "key": "equery.plugin.entropy.sourced",
                            "author": {
                                "security": "LOW TLP:CLEAR",
                                "category": "plugin",
                                "name": "entropy",
                                "version": "1",
                            },
                            "action": "sourced",
                            "num_feature_values": 1,
                        }
                    ],
                    "features": [
                        {
                            "name": "generic_feature",
                            "type": "string",
                            "value": "generic_value",
                            "label": [],
                            "parts": {},
                            "instances": ["equery.plugin.entropy.sourced"],
                        }
                    ],
                    "streams": [
                        {
                            "sha256": "equery",
                            "sha512": "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000equery",
                            "sha1": "0000000000000000000000000000000000equery",
                            "md5": "00000000000000000000000000equery",
                            "ssdeep": "1:1:1",
                            "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                            "size": 1024,
                            "file_format_legacy": "Text",
                            "file_format": "text/plain",
                            "file_extension": "txt",
                            "mime": "text/plain",
                            "magic": "ASCII text",
                            "identify_version": 1,
                            "label": ["content"],
                            "instances": ["equery.plugin.entropy.sourced"],
                        }
                    ],
                    "info": [
                        {
                            "info": {"entropy": {"idk": True, "blocks": [1, 2, 8, 4, 5, 6, 7]}},
                            "instance": "equery.plugin.entropy.sourced",
                        }
                    ],
                    "tags": [
                        {
                            "sha256": "equery",
                            "tag": "t1",
                            "type": "entity_tag",
                            "owner": "generic_owner",
                            "timestamp": "2000-01-01T01:01:01Z",
                            "security": "LOW TLP:CLEAR",
                            "num_entities": 1,
                        },
                        {
                            "sha256": "equery",
                            "tag": "t2",
                            "type": "entity_tag",
                            "owner": "generic_owner",
                            "timestamp": "2000-01-01T01:01:01Z",
                            "security": "LOW TLP:CLEAR",
                            "num_entities": 1,
                        },
                    ],
                },
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )

        # test reading info for specific author
        response = self.client.get("/v0/binaries/equery?detail=info&author=entropy")
        self.assertEqual(response.status_code, 200)
        self.assertFormatted(
            response.json(),
            {
                "data": {
                    "documents": {"count": 1},
                    "info": [
                        {
                            "info": {"entropy": {"idk": True, "blocks": [1, 2, 8, 4, 5, 6, 7]}},
                            "instance": "equery.plugin.entropy.sourced",
                        }
                    ],
                },
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR"},
            },
        )
