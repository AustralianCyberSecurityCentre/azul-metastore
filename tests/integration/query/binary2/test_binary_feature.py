from azul_bedrock import models_restapi

from azul_metastore.query import cache
from azul_metastore.query.binary2 import binary_feature
from tests.support import gen, integration_test


class TestSearchEntity(integration_test.DynamicTestCase):

    def test_features_entity_count(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1"),
                gen.binary_event(eid="e2"),
                gen.binary_event(eid="e1", sourceit=("s2", "2000-01-01T01:01:01Z")),
                gen.binary_event(eid="e2", sourceit=("s1", "2000-01-01T01:01:01Z")),
                gen.binary_event(eid="e1", authornv=("a1", "1")),
                gen.binary_event(eid="e2", authornv=("a1", "1")),
                gen.binary_event(eid="e3", features=[]),
            ]
        )
        fs = binary_feature.count_binaries_with_feature_names(self.writer, ["generic_feature"])[0]
        self.assertFormatted(fs, models_restapi.FeatureMulticountRet(name="generic_feature", entities=2))

    def test_features_value_count(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e2", fvl=[("f1", "v2")]),
                gen.binary_event(eid="e1", sourceit=("s2", "2000-01-01T01:01:01Z"), fvl=[("f1", "v3")]),
                gen.binary_event(eid="e2", sourceit=("s1", "2000-01-01T01:01:01Z"), fvl=[("f1", "v4")]),
                gen.binary_event(eid="e1", authornv=("a1", "1"), fvl=[("f1", "v5")]),
                gen.binary_event(eid="e2", authornv=("a1", "1"), fvl=[("f1", "v6")]),
            ]
        )
        fs = binary_feature.count_values_in_features(self.writer, ["f1"])[0]
        self.assertFormatted(fs, models_restapi.FeatureMulticountRet(name="f1", values=6))

    def test_feature_values_count(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e2", fvl=[("f1", "v2")]),
                gen.binary_event(eid="e1", sourceit=("s2", "2000-01-01T01:01:01Z"), fvl=[("f1", "v3")]),
                gen.binary_event(eid="e2", sourceit=("s1", "2000-01-01T01:01:01Z"), fvl=[("f1", "v4")]),
                gen.binary_event(eid="e1", authornv=("a1", "1"), fvl=[("f1", "v5")]),
                gen.binary_event(eid="e2", authornv=("a1", "1"), fvl=[("f1", "v6")]),
            ]
        )
        fv1 = binary_feature.count_binaries_with_feature_values(
            self.writer, [models_restapi.ValueCountItem(name="f1", value="v1")]
        )[0]
        self.assertFormatted(fv1, models_restapi.ValueCountRet(name="f1", value="v1", entities=1))

    def test_find_feature_values(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e2", fvl=[("f1", "v2")]),
                gen.binary_event(eid="e1", sourceit=("s2", "2000-01-01T01:01:01Z"), fvl=[("f1", "v3")]),
                gen.binary_event(eid="e2", sourceit=("s1", "2000-01-01T01:01:01Z"), fvl=[("f1", "v4")]),
                gen.binary_event(eid="e1", authornv=("a1", "1"), fvl=[("f1", "v5")]),
                gen.binary_event(eid="e2", authornv=("a1", "1"), fvl=[("f1", "v6")]),
                gen.binary_event(
                    eid="e2",
                    authornv=("a3", "1"),
                    fvtl=[("f2", "http://myuser@blah.com:443/this/is/the/path.html?qry&morequer#fragmetnts", "uri")],
                ),
            ]
        )

        ret = binary_feature.find_feature_values(self.writer, "f1")
        self.assertEqual(6, len(ret.values))
        self.assertFormatted(
            ret,
            models_restapi.ReadFeatureValues(
                name="f1",
                type="string",
                values=[
                    models_restapi.ReadFeatureValuesValue(
                        name="f1", value="v1", newest_processed="2021-01-01T12:00:00.000Z"
                    ),
                    models_restapi.ReadFeatureValuesValue(
                        name="f1", value="v2", newest_processed="2021-01-01T12:00:00.000Z"
                    ),
                    models_restapi.ReadFeatureValuesValue(
                        name="f1", value="v3", newest_processed="2021-01-01T12:00:00.000Z"
                    ),
                    models_restapi.ReadFeatureValuesValue(
                        name="f1", value="v4", newest_processed="2021-01-01T12:00:00.000Z"
                    ),
                    models_restapi.ReadFeatureValuesValue(
                        name="f1", value="v5", newest_processed="2021-01-01T12:00:00.000Z"
                    ),
                    models_restapi.ReadFeatureValuesValue(
                        name="f1", value="v6", newest_processed="2021-01-01T12:00:00.000Z"
                    ),
                ],
                is_search_complete=True,
                total=6,
            ),
        )

        ret = binary_feature.find_feature_values(self.writer, "f2", term="http")
        self.assertFormatted(
            ret,
            models_restapi.ReadFeatureValues(
                name="f2",
                type="uri",
                values=[
                    models_restapi.ReadFeatureValuesValue(
                        name="f2",
                        value="http://myuser@blah.com:443/this/is/the/path.html?qry&morequer#fragmetnts",
                        newest_processed="2021-01-01T12:00:00.000Z",
                        score=1,
                    )
                ],
                is_search_complete=True,
                total=1,
            ),
        )

    def test_find_feature_values_large(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvl=[("f1", f"v{x}") for x in range(1000)] + [("f2", f"v1")]),
            ]
        )

        ret = binary_feature.find_feature_values(self.writer, "f2")
        self.assertFormatted(
            ret,
            models_restapi.ReadFeatureValues(
                name="f2",
                type="string",
                values=[
                    models_restapi.ReadFeatureValuesValue(
                        name="f2", value="v1", newest_processed="2021-01-01T12:00:00.000Z"
                    )
                ],
                is_search_complete=True,
                total=1,
            ),
        )

    def test_find_feature_values_combined(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvl=[("f1", "v1"), ("f2", "v999")]),
                gen.binary_event(eid="e2", fvl=[("f1", "v2"), ("f2", "v888")]),
            ]
        )

        ret = binary_feature.find_feature_values(self.writer, "f1")
        self.assertFormatted(
            ret,
            models_restapi.ReadFeatureValues(
                name="f1",
                type="string",
                values=[
                    models_restapi.ReadFeatureValuesValue(
                        name="f1", value="v1", newest_processed="2021-01-01T12:00:00.000Z"
                    ),
                    models_restapi.ReadFeatureValuesValue(
                        name="f1", value="v2", newest_processed="2021-01-01T12:00:00.000Z"
                    ),
                ],
                is_search_complete=True,
                total=2,
            ),
        )

    def test_cache_small_count(self):
        self.writer.man.check_canary(self.writer.sd)

        self.write_binary_events([gen.binary_event(eid="e1", features=[gen.feature(fv=("f1", "v1"))])])
        counted = binary_feature.count_binaries_with_feature_values(
            self.writer, [models_restapi.ValueCountItem(name="f1", value="v1")]
        )
        self.assertFormatted(counted, [models_restapi.ValueCountRet(name="f1", value="v1", entities=1)])
        self.flush()

        # add a new document
        self.write_binary_events([gen.binary_event(eid="e2", features=[gen.feature(fv=("f1", "v1"))])])
        counted = binary_feature.count_binaries_with_feature_values(
            self.writer, [models_restapi.ValueCountItem(name="f1", value="v1")]
        )
        self.assertFormatted(counted, [models_restapi.ValueCountRet(name="f1", value="v1", entities=1)])
        self.flush()

        cache.invalidate_all(self.writer)
        self.flush()
        counted = binary_feature.count_binaries_with_feature_values(
            self.writer, [models_restapi.ValueCountItem(name="f1", value="v1")]
        )
        self.assertFormatted(counted, [models_restapi.ValueCountRet(name="f1", value="v1", entities=2)])
        self.flush()

    def test_many(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e2", fvl=[("f1", "v2")]),
                gen.binary_event(eid="e3", fvl=[("f1", "v3")]),
                gen.binary_event(eid="e4", fvl=[("f1", "v4")]),
                gen.binary_event(eid="e5", fvl=[("f1", "v5")]),
                gen.binary_event(eid="e6", fvl=[("f1", "v6")]),
                gen.binary_event(eid="e7", fvl=[("f1", "v7")]),
                gen.binary_event(eid="e8", fvl=[("f1", "v8")]),
                gen.binary_event(eid="e9", fvl=[("f1", "v9")]),
            ]
        )
        counts = binary_feature.count_binaries_with_feature_values(
            self.writer, [models_restapi.ValueCountItem(name="f1", value=f"v{x}") for x in range(1, 10)]
        )
        self.assertFormatted(
            counts,
            [
                models_restapi.ValueCountRet(name="f1", value="v1", entities=1),
                models_restapi.ValueCountRet(name="f1", value="v2", entities=1),
                models_restapi.ValueCountRet(name="f1", value="v3", entities=1),
                models_restapi.ValueCountRet(name="f1", value="v4", entities=1),
                models_restapi.ValueCountRet(name="f1", value="v5", entities=1),
                models_restapi.ValueCountRet(name="f1", value="v6", entities=1),
                models_restapi.ValueCountRet(name="f1", value="v7", entities=1),
                models_restapi.ValueCountRet(name="f1", value="v8", entities=1),
                models_restapi.ValueCountRet(name="f1", value="v9", entities=1),
            ],
        )

    def test_very_long(self):
        val = "http://blah@tester" + "a" * 1000 + "/test.html"
        hostname = "tester" + "a" * 1000
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvtl=[("f1", val, "uri")]),
            ]
        )
        ret = binary_feature.count_binaries_with_part_values(
            self.writer, [models_restapi.ValuePartCountItem(part="hostname", value=hostname)]
        )[0]
        counts = binary_feature.count_binaries_with_feature_values(
            self.writer, [models_restapi.ValueCountItem(name="f1", value=val)]
        )
        self.assertFormatted(
            ret,
            models_restapi.ValuePartCountRet(
                value="testeraaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                part="hostname",
                entities=1,
            ),
        )
        self.assertFormatted(
            counts,
            [
                models_restapi.ValueCountRet(
                    name="f1",
                    value="http://blah@testeraaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/test.html",
                    entities=1,
                )
            ],
        )

    def test_url(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvtl=[("f1", "http://blah@tester.com", "uri")]),
                gen.binary_event(eid="e2", fvtl=[("f1", "http://blah@tester.com", "uri")]),
                gen.binary_event(eid="e3", fvtl=[("f1", "http://tester.com", "uri")]),
            ]
        )
        ret = binary_feature.count_binaries_with_part_values(
            self.writer, [models_restapi.ValuePartCountItem(part="scheme", value="http")]
        )[0]
        self.assertFormatted(ret, models_restapi.ValuePartCountRet(value="http", part="scheme", entities=3))

        ret = binary_feature.count_binaries_with_part_values(
            self.writer, [models_restapi.ValuePartCountItem(part="hostname", value="tester.com")]
        )[0]
        self.assertFormatted(ret, models_restapi.ValuePartCountRet(value="tester.com", part="hostname", entities=3))

        ret = binary_feature.count_binaries_with_part_values(
            self.writer, [models_restapi.ValuePartCountItem(part="username", value="blah")]
        )[0]
        self.assertFormatted(ret, models_restapi.ValuePartCountRet(value="blah", part="username", entities=2))

        ret = binary_feature.count_binaries_with_part_values(
            self.writer, [models_restapi.ValuePartCountItem(part="hostname", value="gtryhtr")]
        )[0]
        self.assertFormatted(ret, models_restapi.ValuePartCountRet(value="gtryhtr", part="hostname", entities=0))

    def test_feature_value(self):
        # Needed to vary the binary event ID.
        s = [("s2", "2000-01-01T01:0%s:01Z" % s) for s in range(9)]
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1", authornv=("a1", "1"), authorsec=gen.g1_1, fvl=[("f1", "v1")], sourceit=s[0]
                ),
                gen.binary_event(
                    eid="e2", authornv=("a1", "1"), authorsec=gen.g1_1, fvl=[("f1", "v1")], sourceit=s[1]
                ),
                gen.binary_event(
                    eid="e3", authornv=("a1", "1"), authorsec=gen.g2_1, fvl=[("f1", "v1")], sourceit=s[2]
                ),
                gen.binary_event(
                    eid="e1", authornv=("a2", "1"), authorsec=gen.g2_1, fvl=[("f1", "v2")], sourceit=s[3]
                ),
                gen.binary_event(
                    eid="e2", authornv=("a2", "1"), authorsec=gen.g2_1, fvl=[("f1", "v2")], sourceit=s[4]
                ),
                gen.binary_event(
                    eid="e3", authornv=("a2", "1"), authorsec=gen.g2_1, fvl=[("f1", "v2")], sourceit=s[5]
                ),
                gen.binary_event(
                    eid="e3", authornv=("a2", "1"), authorsec=gen.g3_1, fvl=[("f1", "v3")], sourceit=s[6]
                ),
            ]
        )

        # read from feature value (using user 1)
        self.assertFormatted(
            binary_feature.count_binaries_with_feature_names(self.es1, ["f1"]),
            [models_restapi.FeatureMulticountRet(name="f1", entities=2)],
        )
        self.assertFormatted(
            binary_feature.count_values_in_features(self.es1, ["f1"]),
            [models_restapi.FeatureMulticountRet(name="f1", values=1)],
        )
        self.assertFormatted(
            binary_feature.count_binaries_with_feature_values(
                self.es1, [models_restapi.ValueCountItem(name="f1", value="v1")]
            ),
            [models_restapi.ValueCountRet(name="f1", value="v1", entities=2)],
        )
        self.assertFormatted(
            binary_feature.count_binaries_with_feature_values(
                self.es1, [models_restapi.ValueCountItem(name="f1", value="v3")]
            ),
            [models_restapi.ValueCountRet(name="f1", value="v3", entities=0)],
        )

        # read from feature value (using user 2)
        self.assertFormatted(
            binary_feature.count_binaries_with_feature_names(self.es2, ["f1"]),
            [models_restapi.FeatureMulticountRet(name="f1", entities=3)],
        )
        self.assertFormatted(
            binary_feature.count_values_in_features(self.es2, ["f1"]),
            [models_restapi.FeatureMulticountRet(name="f1", values=2)],
        )
        self.assertFormatted(
            binary_feature.count_binaries_with_feature_values(
                self.es2, [models_restapi.ValueCountItem(name="f1", value="v1")]
            ),
            [models_restapi.ValueCountRet(name="f1", value="v1", entities=3)],
        )
        self.assertFormatted(
            binary_feature.count_binaries_with_feature_values(
                self.es2, [models_restapi.ValueCountItem(name="f1", value="v3")]
            ),
            [models_restapi.ValueCountRet(name="f1", value="v3", entities=0)],
        )

        # user es3
        self.assertFormatted(
            binary_feature.count_binaries_with_feature_names(self.es3, ["f1"]),
            [models_restapi.FeatureMulticountRet(name="f1", entities=3)],
        )
        self.assertFormatted(
            binary_feature.count_values_in_features(self.es3, ["f1"]),
            [models_restapi.FeatureMulticountRet(name="f1", values=3)],
        )
        self.assertFormatted(
            binary_feature.count_binaries_with_feature_values(
                self.es3, [models_restapi.ValueCountItem(name="f1", value="v1")]
            ),
            [models_restapi.ValueCountRet(name="f1", value="v1", entities=3)],
        )
        self.assertFormatted(
            binary_feature.count_binaries_with_feature_values(
                self.es3, [models_restapi.ValueCountItem(name="f1", value="v3")]
            ),
            [models_restapi.ValueCountRet(name="f1", value="v3", entities=1)],
        )
