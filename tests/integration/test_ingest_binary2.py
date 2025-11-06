import os
from unittest import mock

from azul_bedrock import models_network as azm
from azul_bedrock import models_restapi

from azul_metastore import ingestor
from azul_metastore.query.binary2 import binary_feature, binary_summary
from tests.support import gen, integration_test

from . import trivial


class TestIngestor(integration_test.DynamicTestCase):

    @classmethod
    def alter_environment(cls):
        super().alter_environment()
        os.environ["metastore_warn_on_event_count"] = str(500)

    def _get_std_events(self) -> list[dict]:
        return [
            gen.binary_event(
                eid="e1",
                authornv=("me", "1"),
                authorsec=gen.g1_1,
                action=azm.BinaryAction.Sourced,
                sourceit=("s1", "2000-01-01T01:01:01Z"),
            ),
            # diff source but same features
            gen.binary_event(
                eid="e1",
                authornv=("me", "1"),
                authorsec=gen.g1_1,
                action=azm.BinaryAction.Sourced,
                sourceit=("s4", "2000-01-01T01:01:01Z"),
            ),
            gen.binary_event(
                eid="e2",
                authornv=("unzip", "1"),
                authorsec=gen.g1_1,
                spathl=[("e1", ("me", "1"))],
                action=azm.BinaryAction.Extracted,
            ),
            # diff source but same features
            gen.binary_event(
                eid="e2",
                authornv=("unzip", "1"),
                authorsec=gen.g1_1,
                spathl=[("e1", ("me", "1"))],
                action=azm.BinaryAction.Extracted,
                sourceit=("s4", "2000-01-01T01:01:01Z"),
            ),
            gen.binary_event(
                eid="e3",
                authornv=("unzip", "1"),
                authorsec=gen.g1_1,
                spathl=[("e1", ("me", "1"))],
                action=azm.BinaryAction.Extracted,
            ),
            gen.binary_event(
                eid="e1",
                authornv=("defilb", "3"),
                authorsec=gen.g1_1,
                spathl=[("e1", ("me", "1"))],
                action=azm.BinaryAction.Enriched,
                sourceit=("s1", "2000-01-01T01:01:01Z"),
            ),
            gen.binary_event(
                eid="e2",
                authornv=("defilb", "3"),
                authorsec=gen.g1_1,
                spathl=[("e1", ("me", "1")), ("e2", ("unzip", "1"))],
                action=azm.BinaryAction.Enriched,
            ),
            gen.binary_event(
                eid="e3",
                authornv=("defilb", "3"),
                authorsec=gen.g1_1,
                spathl=[("e1", ("me", "1")), ("e3", ("unzip", "1"))],
                action=azm.BinaryAction.Enriched,
            ),
            # duplicate plugin result with a different parent
            gen.binary_event(
                eid="e3",
                authornv=("defilb", "3"),
                authorsec=gen.g1_1,
                spathl=[("e1", ("me", "1")), ("e3", ("unzip_dupe", "1"))],
                action=azm.BinaryAction.Enriched,
            ),
            gen.binary_event(
                eid="e4",
                authornv=("defilb", "3"),
                authorsec=gen.g1_1,
                spathl=[("e1", ("me", "1")), ("e2", ("unzip_dupe", "1"))],
                action=azm.BinaryAction.Extracted,
            ),
        ]

    def test_features_value_count(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e2", fvl=[("f1", "v2")]),
                # author should be different
                gen.binary_event(
                    eid="e1", authornv=("a2", "1"), sourceit=("s2", "2000-01-01T01:01:01Z"), fvl=[("f1", "v3")]
                ),
                gen.binary_event(
                    eid="e2", authornv=("a2", "1"), sourceit=("s1", "2000-01-01T01:01:01Z"), fvl=[("f1", "v4")]
                ),
                gen.binary_event(eid="e1", authornv=("a1", "1"), fvl=[("f1", "v5")]),
                gen.binary_event(eid="e2", authornv=("a1", "1"), fvl=[("f1", "v6")]),
            ]
        )
        fs = binary_feature.count_values_in_features(self.writer, ["f1"])[0]
        self.assertFormatted(fs, models_restapi.FeatureMulticountRet(name="f1", values=6))

    def test_features_across_docs(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", fvl=[("f1", "v1")]),
                gen.binary_event(
                    eid="e1", authornv=("a2", "1"), sourceit=("s2", "2000-01-01T01:01:01Z"), fvl=[("f1", "v3")]
                ),
                gen.binary_event(eid="e1", authornv=("a1", "1"), fvl=[("f2", "v5")]),
                gen.binary_event(eid="e2", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e3", fvl=[("f1", "v1")]),
                gen.binary_event(eid="e3", authornv=("a1", "1"), fvl=[("f2", "v5")]),
            ]
        )
        ents = trivial.find_binaries_with_separated_features(self.writer, "f1", "f2")
        self.assertEqual(set(ents), {"e1", "e3"})

    def test_read_metadata(self):
        self.write_binary_events(self._get_std_events())

        ret = binary_summary.read(self.writer, "e1")
        self.assertFormatted(
            ret,
            models_restapi.BinaryMetadata(
                documents=models_restapi.BinaryDocuments(count=3, newest="2021-01-01T12:00:00.000Z"),
                security=["LOW TLP:CLEAR"],
                sources=[
                    models_restapi.BinarySource(
                        source="s1",
                        direct=[
                            models_restapi.EventSource(
                                security="LOW TLP:CLEAR",
                                name="s1",
                                timestamp="2000-01-01T01:01:01Z",
                                references={"ref2": "val2", "ref1": "val1"},
                                track_source_references="s1.dd6e233ae7a843de99f9b43c349069e4",
                            )
                        ],
                    ),
                    models_restapi.BinarySource(
                        source="s4",
                        direct=[
                            models_restapi.EventSource(
                                security="LOW TLP:CLEAR",
                                name="s4",
                                timestamp="2000-01-01T01:01:01Z",
                                references={"ref2": "val2", "ref1": "val1"},
                                track_source_references="s4.dd6e233ae7a843de99f9b43c349069e4",
                            )
                        ],
                    ),
                ],
                children=[
                    models_restapi.PathNode(
                        sha256="e2",
                        action=azm.BinaryAction.Extracted,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(category="plugin", name="unzip", version="1", security="LOW TLP:CLEAR"),
                        relationship={"random": "data", "action": "extracted", "label": "within"},
                        file_format_legacy="Text",
                        file_format="text/plain",
                        size=1024,
                        track_link="e1.e2.plugin.unzip.1",
                    ),
                    models_restapi.PathNode(
                        sha256="e3",
                        action=azm.BinaryAction.Extracted,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(category="plugin", name="unzip", version="1", security="LOW TLP:CLEAR"),
                        relationship={"random": "data", "action": "extracted", "label": "within"},
                        file_format_legacy="Text",
                        file_format="text/plain",
                        size=1024,
                        track_link="e1.e3.plugin.unzip.1",
                    ),
                ],
                instances=[
                    models_restapi.EntityInstance(
                        key="e1.plugin.defilb.enriched",
                        author=models_restapi.EntityInstanceAuthor(
                            security="LOW TLP:CLEAR", category="plugin", name="defilb", version="3"
                        ),
                        action=azm.BinaryAction.Enriched,
                        num_feature_values=1,
                    ),
                    models_restapi.EntityInstance(
                        key="e1.plugin.me.sourced",
                        author=models_restapi.EntityInstanceAuthor(
                            security="LOW TLP:CLEAR", category="plugin", name="me", version="1"
                        ),
                        action=azm.BinaryAction.Sourced,
                        num_feature_values=1,
                    ),
                ],
                features=[
                    models_restapi.BinaryFeatureValue(
                        name="generic_feature",
                        type=azm.FeatureType.String,
                        value="generic_value",
                        parts=models_restapi.FeatureValuePart(),
                        instances=["e1.plugin.defilb.enriched", "e1.plugin.me.sourced"],
                    )
                ],
                streams=[
                    models_restapi.DatastreamInstances(
                        sha256="e1",
                        sha512="000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e1",
                        sha1="00000000000000000000000000000000000000e1",
                        md5="000000000000000000000000000000e1",
                        ssdeep="1:1:1",
                        tlsh="T10000000000000000000000000000000000000000000000000000000000000000000000",
                        size=1024,
                        file_format_legacy="Text",
                        file_format="text/plain",
                        file_extension="txt",
                        mime="text/plain",
                        magic="ASCII text",
                        identify_version=1,
                        label=["content"],
                        instances=["e1.plugin.me.sourced"],
                    )
                ],
            ),
        )

        ret = binary_summary.read(self.writer, "e2")
        self.assertFormatted(
            ret,
            models_restapi.BinaryMetadata(
                documents=models_restapi.BinaryDocuments(count=3, newest="2021-01-01T12:00:00.000Z"),
                security=["LOW TLP:CLEAR"],
                sources=[
                    models_restapi.BinarySource(
                        source="generic_source",
                        indirect=[
                            models_restapi.EventSource(
                                security="LOW TLP:CLEAR",
                                name="generic_source",
                                timestamp="2021-01-01T11:00:00Z",
                                references={"ref2": "val2", "ref1": "val1"},
                                track_source_references="generic_source.dd6e233ae7a843de99f9b43c349069e4",
                            )
                        ],
                    ),
                    models_restapi.BinarySource(
                        source="s4",
                        indirect=[
                            models_restapi.EventSource(
                                security="LOW TLP:CLEAR",
                                name="s4",
                                timestamp="2000-01-01T01:01:01Z",
                                references={"ref2": "val2", "ref1": "val1"},
                                track_source_references="s4.dd6e233ae7a843de99f9b43c349069e4",
                            )
                        ],
                    ),
                ],
                parents=[
                    models_restapi.PathNode(
                        sha256="e1",
                        action=azm.BinaryAction.Extracted,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(category="plugin", name="unzip", version="1", security="LOW TLP:CLEAR"),
                        relationship={"random": "data", "action": "extracted", "label": "within"},
                        file_format_legacy="Text",
                        file_format="text/plain",
                        size=1024,
                        track_link="e1.e2.plugin.unzip.1",
                    )
                ],
                children=[
                    models_restapi.PathNode(
                        sha256="e4",
                        action=azm.BinaryAction.Extracted,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(category="plugin", name="defilb", version="3", security="LOW TLP:CLEAR"),
                        relationship={"random": "data", "action": "extracted", "label": "within"},
                        file_format_legacy="Text",
                        file_format="text/plain",
                        size=1024,
                        track_link="e2.e4.plugin.defilb.3",
                    )
                ],
                instances=[
                    models_restapi.EntityInstance(
                        key="e2.plugin.defilb.enriched",
                        author=models_restapi.EntityInstanceAuthor(
                            security="LOW TLP:CLEAR", category="plugin", name="defilb", version="3"
                        ),
                        action=azm.BinaryAction.Enriched,
                        num_feature_values=1,
                    ),
                    models_restapi.EntityInstance(
                        key="e2.plugin.unzip.extracted",
                        author=models_restapi.EntityInstanceAuthor(
                            security="LOW TLP:CLEAR", category="plugin", name="unzip", version="1"
                        ),
                        action=azm.BinaryAction.Extracted,
                        num_feature_values=1,
                    ),
                ],
                features=[
                    models_restapi.BinaryFeatureValue(
                        name="generic_feature",
                        type=azm.FeatureType.String,
                        value="generic_value",
                        parts=models_restapi.FeatureValuePart(),
                        instances=["e2.plugin.defilb.enriched", "e2.plugin.unzip.extracted"],
                    )
                ],
                streams=[
                    models_restapi.DatastreamInstances(
                        sha256="e2",
                        sha512="000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2",
                        sha1="00000000000000000000000000000000000000e2",
                        md5="000000000000000000000000000000e2",
                        ssdeep="1:1:1",
                        tlsh="T10000000000000000000000000000000000000000000000000000000000000000000000",
                        size=1024,
                        file_format_legacy="Text",
                        file_format="text/plain",
                        file_extension="txt",
                        mime="text/plain",
                        magic="ASCII text",
                        identify_version=1,
                        label=["content"],
                        instances=["e2.plugin.unzip.extracted"],
                    )
                ],
            ),
        )

    @mock.patch("azul_metastore.ingestor.BaseIngestor.is_done")
    @mock.patch("azul_metastore.ingestor.BaseIngestor.get_data")
    def test_binary(self, _get_data, _is_done):
        # self.writer.sd.enable_log_es_queries = True
        self.write_plugin_events()
        _get_data.return_value = self._get_std_events()

        _is_done.side_effect = [False, True]
        ing = ingestor.BinaryIngestor(self.writer)
        ing.main()
        self.flush()

        trivial.summarise_links(self.writer)
        trivial.summarise_results(self.writer)
        trivial.summarise_submissions(self.writer)
        self.assertEqual(4, trivial.count_total_binary_links(self.writer))
        self.assertEqual(9, trivial.count_total_binary_results(self.writer))
        self.assertEqual(6, trivial.count_total_binary_submissions(self.writer))
        self.assertEqual(4, self.count_binary_events("e1"))

        result1 = binary_summary.read(self.writer, "e1")
        self.assertFormatted(
            result1,
            models_restapi.BinaryMetadata(
                documents=models_restapi.BinaryDocuments(count=3, newest="2021-01-01T12:00:00.000Z"),
                security=["LOW TLP:CLEAR"],
                sources=[
                    models_restapi.BinarySource(
                        source="s1",
                        direct=[
                            models_restapi.EventSource(
                                security="LOW TLP:CLEAR",
                                name="s1",
                                timestamp="2000-01-01T01:01:01Z",
                                references={"ref2": "val2", "ref1": "val1"},
                                track_source_references="s1.dd6e233ae7a843de99f9b43c349069e4",
                            )
                        ],
                    ),
                    models_restapi.BinarySource(
                        source="s4",
                        direct=[
                            models_restapi.EventSource(
                                security="LOW TLP:CLEAR",
                                name="s4",
                                timestamp="2000-01-01T01:01:01Z",
                                references={"ref2": "val2", "ref1": "val1"},
                                track_source_references="s4.dd6e233ae7a843de99f9b43c349069e4",
                            )
                        ],
                    ),
                ],
                children=[
                    models_restapi.PathNode(
                        sha256="e2",
                        action=azm.BinaryAction.Extracted,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(category="plugin", name="unzip", version="1", security="LOW TLP:CLEAR"),
                        relationship={"random": "data", "action": "extracted", "label": "within"},
                        file_format_legacy="Text",
                        file_format="text/plain",
                        size=1024,
                        track_link="e1.e2.plugin.unzip.1",
                    ),
                    models_restapi.PathNode(
                        sha256="e3",
                        action=azm.BinaryAction.Extracted,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(category="plugin", name="unzip", version="1", security="LOW TLP:CLEAR"),
                        relationship={"random": "data", "action": "extracted", "label": "within"},
                        file_format_legacy="Text",
                        file_format="text/plain",
                        size=1024,
                        track_link="e1.e3.plugin.unzip.1",
                    ),
                ],
                instances=[
                    models_restapi.EntityInstance(
                        key="e1.plugin.defilb.enriched",
                        author=models_restapi.EntityInstanceAuthor(
                            security="LOW TLP:CLEAR", category="plugin", name="defilb", version="3"
                        ),
                        action=azm.BinaryAction.Enriched,
                        num_feature_values=1,
                    ),
                    models_restapi.EntityInstance(
                        key="e1.plugin.me.sourced",
                        author=models_restapi.EntityInstanceAuthor(
                            security="LOW TLP:CLEAR", category="plugin", name="me", version="1"
                        ),
                        action=azm.BinaryAction.Sourced,
                        num_feature_values=1,
                    ),
                ],
                features=[
                    models_restapi.BinaryFeatureValue(
                        name="generic_feature",
                        type=azm.FeatureType.String,
                        value="generic_value",
                        parts=models_restapi.FeatureValuePart(),
                        instances=["e1.plugin.defilb.enriched", "e1.plugin.me.sourced"],
                    )
                ],
                streams=[
                    models_restapi.DatastreamInstances(
                        sha256="e1",
                        sha512="000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e1",
                        sha1="00000000000000000000000000000000000000e1",
                        md5="000000000000000000000000000000e1",
                        ssdeep="1:1:1",
                        tlsh="T10000000000000000000000000000000000000000000000000000000000000000000000",
                        size=1024,
                        file_format_legacy="Text",
                        file_format="text/plain",
                        file_extension="txt",
                        mime="text/plain",
                        magic="ASCII text",
                        identify_version=1,
                        label=["content"],
                        instances=["e1.plugin.me.sourced"],
                    )
                ],
            ),
        )

        result2 = binary_summary.read(self.writer, "e2")
        self.assertFormatted(
            result2,
            models_restapi.BinaryMetadata(
                documents=models_restapi.BinaryDocuments(count=3, newest="2021-01-01T12:00:00.000Z"),
                security=["LOW TLP:CLEAR"],
                sources=[
                    models_restapi.BinarySource(
                        source="generic_source",
                        indirect=[
                            models_restapi.EventSource(
                                security="LOW TLP:CLEAR",
                                name="generic_source",
                                timestamp="2021-01-01T11:00:00Z",
                                references={"ref2": "val2", "ref1": "val1"},
                                track_source_references="generic_source.dd6e233ae7a843de99f9b43c349069e4",
                            )
                        ],
                    ),
                    models_restapi.BinarySource(
                        source="s4",
                        indirect=[
                            models_restapi.EventSource(
                                security="LOW TLP:CLEAR",
                                name="s4",
                                timestamp="2000-01-01T01:01:01Z",
                                references={"ref2": "val2", "ref1": "val1"},
                                track_source_references="s4.dd6e233ae7a843de99f9b43c349069e4",
                            )
                        ],
                    ),
                ],
                parents=[
                    models_restapi.PathNode(
                        sha256="e1",
                        action=azm.BinaryAction.Extracted,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(category="plugin", name="unzip", version="1", security="LOW TLP:CLEAR"),
                        relationship={"random": "data", "action": "extracted", "label": "within"},
                        file_format_legacy="Text",
                        file_format="text/plain",
                        size=1024,
                        track_link="e1.e2.plugin.unzip.1",
                    )
                ],
                children=[
                    models_restapi.PathNode(
                        sha256="e4",
                        action=azm.BinaryAction.Extracted,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(category="plugin", name="defilb", version="3", security="LOW TLP:CLEAR"),
                        relationship={"random": "data", "action": "extracted", "label": "within"},
                        file_format_legacy="Text",
                        file_format="text/plain",
                        size=1024,
                        track_link="e2.e4.plugin.defilb.3",
                    )
                ],
                instances=[
                    models_restapi.EntityInstance(
                        key="e2.plugin.defilb.enriched",
                        author=models_restapi.EntityInstanceAuthor(
                            security="LOW TLP:CLEAR", category="plugin", name="defilb", version="3"
                        ),
                        action=azm.BinaryAction.Enriched,
                        num_feature_values=1,
                    ),
                    models_restapi.EntityInstance(
                        key="e2.plugin.unzip.extracted",
                        author=models_restapi.EntityInstanceAuthor(
                            security="LOW TLP:CLEAR", category="plugin", name="unzip", version="1"
                        ),
                        action=azm.BinaryAction.Extracted,
                        num_feature_values=1,
                    ),
                ],
                features=[
                    models_restapi.BinaryFeatureValue(
                        name="generic_feature",
                        type=azm.FeatureType.String,
                        value="generic_value",
                        parts=models_restapi.FeatureValuePart(),
                        instances=["e2.plugin.defilb.enriched", "e2.plugin.unzip.extracted"],
                    )
                ],
                streams=[
                    models_restapi.DatastreamInstances(
                        sha256="e2",
                        sha512="000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2",
                        sha1="00000000000000000000000000000000000000e2",
                        md5="000000000000000000000000000000e2",
                        ssdeep="1:1:1",
                        tlsh="T10000000000000000000000000000000000000000000000000000000000000000000000",
                        size=1024,
                        file_format_legacy="Text",
                        file_format="text/plain",
                        file_extension="txt",
                        mime="text/plain",
                        magic="ASCII text",
                        identify_version=1,
                        label=["content"],
                        instances=["e2.plugin.unzip.extracted"],
                    )
                ],
            ),
        )
