from azul_bedrock import models_network as azm
from azul_bedrock import models_restapi

from azul_metastore.query.binary2 import binary_find
from tests.support import gen, integration_test


class TestEntityFind(integration_test.DynamicTestCase):

    def test_capture_unmatched_source(self):
        # check can summarise info about the binary that doesn't match the search term
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    sourceit=("s1", "2000-01-01T00:00:00Z"),
                    authornv=("a1", "1"),
                    fvl=[("filename", "thing.exe")],
                    spathl=[("emimep", ("relation", "1"))],
                ),
                gen.binary_event(
                    eid="e1",
                    sourceit=("s2", "2020-01-01T00:00:00Z"),
                    authornv=("a2", "1"),
                    fvl=[("filename", "other.exe")],
                    spathl=[("emimep", ("relation", "1"))],
                ),
            ],
        )
        res = binary_find.find_binaries(self.writer, term='features_map.filename:"thing.exe"')
        self.assertEqual(1, len(res.items))
        self.assertEqual({"features_map.filename": ["thing.exe"]}, res.items[0].highlight)
        self.assertEqual(["thing.exe", "other.exe"], res.items[0].filenames)
        self.assertEqual(["s1", "s2"], [x.name for x in res.items[0].sources])

    def test_reads_mime_ok(self):
        bf = [
            ("magic", "ASCII text"),
            ("mime", "text/plain"),
            ("filename", "text.txt"),
        ]
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="emime",
                    sourceit=("s1", "2000-01-01T00:00:00Z"),
                    authornv=("a1", "1"),
                    fvl=[],
                    spathl=[("emimep", ("relation", "1"))],
                ),
                gen.binary_event(
                    eid="emime",
                    sourceit=("s1", "2020-01-01T00:00:00Z"),
                    authornv=("a2", "1"),
                    fvl=[],
                    spathl=[("emimep", ("relation", "1"))],
                ),
                gen.binary_event(
                    eid="emime", sourceit=("s1", "2010-01-01T00:00:00Z"), authornv=("a3", "1"), fvl=bf, spathl=[]
                ),
            ],
        )
        res = binary_find.find_binaries(self.writer)
        self.assertEqual(1, len(res.items))
        self.assertEqual("ASCII text", res.items[0].magic)
        self.assertEqual("text/plain", res.items[0].mime)
        self.assertEqual(["text.txt"], res.items[0].filenames)

    def test_hashes_inorder(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e2", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e3", fvl=[("f1", "v1")]),
            ]
        )
        ret = binary_find.find_binaries(self.writer, hashes=["e1"])
        self.assertEqual(1, len(ret.items))
        # must keep same order
        ret = binary_find.find_binaries(self.writer, hashes=["e3", "e1", "invalid", "e2"])
        self.assertEqual(4, len(ret.items))
        self.assertEqual(["e3", "e1", "invalid", "e2"], [x.key for x in ret.items])
        self.assertEqual([True, True, False, True], [x.exists for x in ret.items])

        # ensure that term search works with hashes and highlighting
        ret = binary_find.find_binaries(self.writer, hashes=["e3", "e1", "e2"], term='features_map.f1:"v1"')
        self.assertEqual(3, len(ret.items))
        self.assertEqual(["e3", "e1", "e2"], [x.key for x in ret.items])
        self.assertEqual([True, True, True], [x.exists for x in ret.items])
        self.assertFormatted(
            ret.items[0],
            models_restapi.EntityFindItem(
                key="e3",
                exists=True,
                has_content=True,
                sources=[
                    models_restapi.EntityFindItemSource(
                        depth=0,
                        name="generic_source",
                        timestamp="2021-01-01T11:00:00Z",
                        references={"ref2": "val2", "ref1": "val1"},
                    )
                ],
                file_size=1024,
                file_format="text/plain",
                file_extension="txt",
                magic="ASCII text",
                mime="text/plain",
                highlight={"features_map.f1": ["v1"]},
                md5="000000000000000000000000000000e3",
                sha1="00000000000000000000000000000000000000e3",
                sha256="e3",
                sha512="000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e3",
                ssdeep="1:1:1",
                tlsh="T10000000000000000000000000000000000000000000000000000000000000000000000",
            ),
        )

    def test_hashes_md5_collide(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", post_patch={"entity": {"md5": "milk"}}),
                gen.binary_event(eid="e2", post_patch={"entity": {"md5": "milk"}}),
                gen.binary_event(eid="e3", post_patch={"entity": {"md5": "milk"}}),
            ]
        )
        ret = binary_find.find_binaries(self.writer, hashes=["milk"])
        self.assertEqual(3, len(ret.items))
        self.assertEqual(["milk", "milk", "milk"], [x.key for x in ret.items])
        self.assertEqual([True, True, True], [x.exists for x in ret.items])

    def test_search_with_md5_and_sha(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", post_patch={"entity": {"md5": "md1", "sha256": "e1", "sha512": "abc1"}}),
                gen.binary_event(eid="e2", post_patch={"entity": {"md5": "md2", "sha256": "e2", "sha512": "abc2"}}),
                gen.binary_event(eid="e3", post_patch={"entity": {"md5": "md3", "sha256": "e3", "sha512": "abc3"}}),
            ]
        )
        ret = binary_find.find_binaries(self.writer, hashes=["md1", "e1", "abc1", "md5555"])

        dict_vals = {i.key: i for i in ret.items}
        self.assertEqual(4, len(ret.items))
        self.assertEqual(dict_vals["e1"].exists, True)
        self.assertGreaterEqual(len(dict_vals["e1"].sources), 1)
        self.assertEqual(dict_vals["e1"].is_duplicate_find, None)

        self.assertEqual(dict_vals["md1"].exists, True)
        self.assertEqual(dict_vals["md1"].has_content, True)
        self.assertEqual(dict_vals["md1"].is_duplicate_find, True)

        self.assertEqual(dict_vals["abc1"].exists, True)
        self.assertEqual(dict_vals["abc1"].has_content, True)
        self.assertEqual(dict_vals["abc1"].is_duplicate_find, True)

        self.assertEqual(dict_vals["md5555"].exists, False)

    def test_reads_mime_ok_hashes(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="emime",
                    sourceit=("s1", "2000-01-01T00:00:00Z"),
                    authornv=("a1", "1"),
                    fvl=[],
                    spathl=[("emimep", ("relation", "1"))],
                ),
                gen.binary_event(
                    eid="emime",
                    sourceit=("s1", "2020-01-01T00:00:00Z"),
                    authornv=("a2", "1"),
                    fvl=[],
                    spathl=[("emimep", ("relation", "1"))],
                ),
                # Test to see if partial event is present (no magic, the magic value is still acquired.)
                gen.binary_event(
                    eid="emime",
                    sourceit=("s1", "2010-01-01T00:00:00Z"),
                    authornv=("a3", "1"),
                    fvl=[
                        ("mime", "text/plain"),
                        ("filename", "text.txt"),
                    ],
                    spathl=[],
                ),
                gen.binary_event(
                    eid="emime",
                    sourceit=("s1", "2010-01-01T00:00:00Z"),
                    authornv=("a4", "1"),
                    fvl=[
                        ("magic", "ASCII text"),
                        ("mime", "text/plain"),
                        ("filename", "text.txt"),
                    ],
                    magicmime=("ASCII text", "text/plain"),
                    spathl=[],
                ),
            ],
        )
        res = binary_find.find_binaries(self.writer, hashes=["emime"]).items
        self.assertEqual(1, len(res))
        self.assertEqual("ASCII text", res[0].magic)
        self.assertEqual("text/plain", res[0].mime)
        self.assertEqual(["text.txt"], res[0].filenames)

    def test_newest_source_timestamp_hashes(self):
        self.write_binary_events([gen.binary_event(eid="e1", sourceit=("s1", "2000-01-01T00:00:00Z"))])
        self.write_binary_events([gen.binary_event(eid="e1", sourceit=("s1", "2010-01-01T00:00:00Z"))])
        self.write_binary_events([gen.binary_event(eid="e1", sourceit=("s1", "2020-01-01T00:00:00Z"))])
        ret = binary_find.find_binaries(self.writer, hashes=["e1"]).items
        self.assertEqual("2020-01-01T00:00:00Z", ret[0].sources[0].timestamp)

    def test_count(self):
        self.write_binary_events(
            [gen.binary_event(eid=f"e{x}", sourceit=("s1", "2000-01-01T00:00:00Z")) for x in range(100)]
        )
        ret = binary_find.find_binaries(self.writer, max_binaries=10, count_binaries=True)
        self.assertEqual(ret.items_count, 100)
        self.assertEqual(len(ret.items), 10)
        ret = binary_find.find_binaries(self.writer, max_binaries=10)
        self.assertEqual(ret.items_count, 0)
        self.assertEqual(len(ret.items), 10)
        ret = binary_find.find_binaries(self.writer, max_binaries=0, count_binaries=True)
        self.assertEqual(ret.items_count, 100)
        self.assertEqual(len(ret.items), 0)

        # cross the accurate count threshold
        self.write_binary_events(
            [gen.binary_event(eid=f"e{x}", sourceit=("s1", "2000-01-01T00:00:00Z")) for x in range(100, 200)]
        )
        ret = binary_find.find_binaries(self.writer, max_binaries=10, count_binaries=True)
        self.assertGreater(ret.items_count, 180)
        self.assertEqual(len(ret.items), 10)
        ret = binary_find.find_binaries(self.writer, max_binaries=10)
        self.assertNotIn("items_count", ret)
        self.assertEqual(len(ret.items), 10)
        ret = binary_find.find_binaries(self.writer, max_binaries=0, count_binaries=True)
        self.assertGreater(ret.items_count, 180)
        self.assertEqual(len(ret.items), 0)

    def test_read_entities_quick(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    authornv=("a1", "1"),
                    fvl=[
                        ("magic", "ASCII text"),
                        ("mime", "application/sql"),
                        ("filename", "a.exe"),
                        ("filename", "b.exe"),
                        ("filename", "c.exe"),
                    ],
                    magicmime=("ASCII text", "application/sql"),
                )
            ]
        )

        ret = binary_find.find_binaries(self.writer, hashes=["e1"]).items

        self.assertEqual(1, len(ret))
        self.assertEqual("e1", ret[0].sha256)
        self.assertEqual(True, ret[0].exists)
        self.assertEqual(1024, ret[0].file_size)
        self.assertEqual("ASCII text", ret[0].magic)
        self.assertEqual("application/sql", ret[0].mime)
        self.assertEqual(3, len(ret[0].filenames))
        self.assertEqual(0, ret[0].sources[0].depth)
        self.assertEqual("generic_source", ret[0].sources[0].name)

        ret = binary_find.find_binaries(self.writer, hashes=["invalid1"]).items

        self.assertEqual(1, len(ret))
        self.assertEqual(False, ret[0].exists)

        ret = binary_find.find_binaries(self.writer, hashes=["E1"]).items

        self.assertEqual(1, len(ret))
        self.assertEqual("e1", ret[0].sha256)
        self.assertEqual(True, ret[0].exists)
        self.assertEqual(1024, ret[0].file_size)
        self.assertEqual("ASCII text", ret[0].magic)
        self.assertEqual("application/sql", ret[0].mime)
        self.assertEqual(3, len(ret[0].filenames))
        self.assertEqual(0, ret[0].sources[0].depth)
        self.assertEqual("generic_source", ret[0].sources[0].name)

        ret = binary_find.find_binaries(self.writer, hashes=["INVALID1"]).items

        self.assertEqual(1, len(ret))
        self.assertEqual(False, ret[0].exists)

    def test_read_entities_quick_ssdeep(self):
        # check that we can read ssdeep property
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    authornv=("a2", "1"),
                    ssdeep="384:EWo4X1WaPW9ZWhWzLo+lWpct/fWbkWsWIwW0/S7dZhgG8:EWo4X1WmW9ZWhWH/WpchfWgWsWTWtf8",
                ),
            ]
        )

        ret = binary_find.find_binaries(self.writer, hashes=["e1"]).items

        self.assertEqual(1, len(ret))
        self.assertEqual("e1", ret[0].sha256)
        self.assertEqual(True, ret[0].exists)
        self.assertEqual(
            "384:EWo4X1WaPW9ZWhWzLo+lWpct/fWbkWsWIwW0/S7dZhgG8:EWo4X1WmW9ZWhWH/WpchfWgWsWTWtf8", ret[0].ssdeep
        )

    def test_highlight(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("tester", "1"), fvl=[("f1", "v1")]),
            ]
        )
        # full author
        ret = binary_find.find_binaries(self.writer, term='author.name:"tester"').items
        item1 = ret[0]
        self.assertEqual("e1", item1.sha256)
        self.assertEqual(True, item1.exists)
        self.assertEqual({"author.name": ["tester"]}, item1.highlight)

        # partial author
        ret = binary_find.find_binaries(self.writer, term='author.name:"test*"').items
        item1 = ret[0]
        self.assertEqual("e1", item1.sha256)
        self.assertEqual(True, item1.exists)
        self.assertEqual({"author.name": ["tester"]}, item1.highlight)

        # file format with special character
        ret = binary_find.find_binaries(self.writer, term='"text/"').items
        item1 = ret[0]
        self.assertEqual("e1", item1.sha256)
        self.assertEqual(True, item1.exists)
        self.assertEqual({"file_format": ["text/plain"], "mime": ["text/plain"]}, item1.highlight)

        # hash
        ret = binary_find.find_binaries(self.writer, hashes=["e1"], term='features_map.f1:"v1"').items
        item1 = ret[0]
        self.assertEqual("e1", item1.sha256)
        self.assertEqual(True, item1.exists)
        self.assertEqual({"features_map.f1": ["v1"]}, item1.highlight)

    def test_has_content(self):
        self.write_binary_events(
            [
                # check simple cases
                gen.binary_event(eid="e1", datas=[gen.data({"label": azm.DataLabel.CONTENT})]),
                gen.binary_event(eid="e2", datas=[gen.data({"label": azm.DataLabel.TEST})]),
                gen.binary_event(
                    eid="e3",
                    datas=[gen.data({"label": azm.DataLabel.CONTENT}), gen.data({"label": azm.DataLabel.TEST})],
                ),
                gen.binary_event(
                    eid="e4",
                    datas=[gen.data({"label": azm.DataLabel.TEST}), gen.data({"label": azm.DataLabel.CONTENT})],
                ),
                gen.binary_event(eid="e5", datas=[]),
                # create many events for one entity
                gen.binary_event(eid="e99", authornv=("1", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("2", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("3", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("4", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("5", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("6", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("7", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("8", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("9", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("11", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("12", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("13", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("14", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("15", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("16", "1"), datas=[]),
                gen.binary_event(
                    eid="e99",
                    authornv=("17", "1"),
                    datas=[gen.data({"label": azm.DataLabel.TEST}), gen.data({"label": azm.DataLabel.CONTENT})],
                ),
                gen.binary_event(eid="e99", authornv=("18", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("19", "1"), datas=[]),
                gen.binary_event(eid="e99", authornv=("1", "1"), datas=[]),
                # create many events for one entity
                gen.binary_event(eid="e87", authornv=("1", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("2", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("3", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("4", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("5", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("6", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("7", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("8", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("9", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("11", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("12", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("13", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("14", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("15", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("16", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("17", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("18", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("19", "1"), datas=[]),
                gen.binary_event(eid="e87", authornv=("1", "1"), datas=[]),
            ]
        )

        ret = binary_find.find_binaries(self.writer, hashes=["e1"]).items[0]
        self.assertEqual("e1", ret.sha256)
        self.assertEqual(True, ret.exists)
        self.assertEqual(True, ret.has_content)

        ret = binary_find.find_binaries(self.writer, hashes=["e2"]).items[0]
        self.assertEqual("e2", ret.sha256)
        self.assertEqual(True, ret.exists)
        self.assertEqual(False, ret.has_content)

        ret = binary_find.find_binaries(self.writer, hashes=["e3"]).items[0]
        self.assertEqual("e3", ret.sha256)
        self.assertEqual(True, ret.exists)
        self.assertEqual(True, ret.has_content)

        ret = binary_find.find_binaries(self.writer, hashes=["e4"]).items[0]
        self.assertEqual("e4", ret.sha256)
        self.assertEqual(True, ret.exists)
        self.assertEqual(True, ret.has_content)

        ret = binary_find.find_binaries(self.writer, hashes=["e5"]).items[0]
        self.assertEqual("e5", ret.sha256)
        self.assertEqual(True, ret.exists)
        self.assertEqual(False, ret.has_content)

        ret = binary_find.find_binaries(self.writer, hashes=["e99"]).items[0]
        self.assertEqual("e99", ret.sha256)
        self.assertEqual(True, ret.exists)
        self.assertEqual(True, ret.has_content)

        ret = binary_find.find_binaries(self.writer, hashes=["e87"]).items[0]
        self.assertEqual("e87", ret.sha256)
        self.assertEqual(True, ret.exists)
        self.assertEqual(False, ret.has_content)

    def test_cross_doc_search(self):
        # check that we can read ssdeep property
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    fvl=[("f1", "v1")],
                    authornv=("a1", "1"),
                ),
                gen.binary_event(
                    eid="e1",
                    fvl=[("f2", "v2")],
                    authornv=("a2", "1"),
                ),
                gen.binary_event(
                    eid="e1",
                    fvl=[("f3", "v3")],
                    authornv=("a3", "1"),
                ),
                gen.binary_event(
                    eid="e2",
                    fvl=[("f2", "v2")],
                    authornv=("a2", "1"),
                ),
                gen.binary_event(
                    eid="e2",
                    fvl=[("f3", "v3")],
                    authornv=("a3", "1"),
                ),
                gen.binary_event(
                    eid="e3",
                    fvl=[("f1", "v1")],
                    authornv=("a1", "1"),
                ),
                gen.binary_event(
                    eid="e3",
                    fvl=[("f2", "v2")],
                    authornv=("a2", "1"),
                ),
            ]
        )

        ret = binary_find.find_binaries(self.writer, term='features_map.f1:"v1"').items
        self.assertEqual(2, len(ret))
        self.assertEqual({"e1", "e3"}, {x.sha256 for x in ret})
        self.assertFormatted(ret[0].highlight, {"features_map.f1": ["v1"]})

        ret = binary_find.find_binaries(self.writer, term='features_map.f1:"v1" AND features_map.f2:"v2"').items
        self.assertEqual(2, len(ret))
        self.assertEqual({"e1", "e3"}, {x.sha256 for x in ret})
        self.assertFormatted(ret[0].highlight, {"features_map.f1": ["v1"], "features_map.f2": ["v2"]})

        ret = binary_find.find_binaries(
            self.writer, term='features_map.f1:"v1" AND features_map.f2:"v2" AND features_map.f3:"v3"'
        ).items
        self.assertEqual(1, len(ret))
        self.assertEqual({"e1"}, {x.sha256 for x in ret})
        self.assertFormatted(
            ret[0].highlight, {"features_map.f1": ["v1"], "features_map.f2": ["v2"], "features_map.f3": ["v3"]}
        )
