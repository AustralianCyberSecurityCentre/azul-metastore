from azul_bedrock import models_network as azm
from azul_bedrock import models_restapi

from azul_metastore.query.binary2 import binary_summary
from tests.support import gen, integration_test


class TestBinaryRead(integration_test.DynamicTestCase):

    def test_read_all(self):
        self.write_binary_events(
            [
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
                    action=azm.BinaryAction.Mapped,
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
                    action=azm.BinaryAction.Mapped,
                    sourceit=("s4", "2000-01-01T01:01:01Z"),
                ),
                # same parent with different plugin
                gen.binary_event(
                    eid="e2",
                    authornv=("lief", "1"),
                    authorsec=gen.g1_1,
                    spathl=[("e1", ("me", "1"))],
                    action=azm.BinaryAction.Extracted,
                    timestamp="2025-01-01T01:01:01Z",  # ensure this event is indexed before unzip.
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
        )
        ret = binary_summary.read(self.writer, "e2")
        self.assertFormatted(
            ret,
            models_restapi.BinaryMetadata(
                documents=models_restapi.BinaryDocuments(count=4, newest="2025-01-01T01:01:01.000Z"),
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
                        timestamp="2025-01-01T01:01:01Z",
                        author=azm.Author(category="plugin", name="lief", version="1", security="LOW TLP:CLEAR"),
                        relationship={"random": "data", "action": "extracted", "label": "within"},
                        file_format_legacy="Text",
                        file_format="text/plain",
                        size=1024,
                        track_link="e1.e2.plugin.lief.1",
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
                        key="e2.plugin.lief.extracted",
                        author=models_restapi.EntityInstanceAuthor(
                            security="LOW TLP:CLEAR", category="plugin", name="lief", version="1"
                        ),
                        action=azm.BinaryAction.Extracted,
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
                    models_restapi.EntityInstance(
                        key="e2.plugin.unzip.mapped",
                        author=models_restapi.EntityInstanceAuthor(
                            security="LOW TLP:CLEAR", category="plugin", name="unzip", version="1"
                        ),
                        action=azm.BinaryAction.Mapped,
                        num_feature_values=1,
                    ),
                ],
                features=[
                    models_restapi.BinaryFeatureValue(
                        name="generic_feature",
                        type=azm.FeatureType.String,
                        value="generic_value",
                        parts=models_restapi.FeatureValuePart(),
                        instances=[
                            "e2.plugin.defilb.enriched",
                            "e2.plugin.lief.extracted",
                            "e2.plugin.unzip.extracted",
                            "e2.plugin.unzip.mapped",
                        ],
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
                        instances=["e2.plugin.lief.extracted", "e2.plugin.unzip.extracted"],
                    )
                ],
            ),
        )

    def test_read_sources(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"a": "2"}),
                gen.binary_event(
                    # enriched events shouldn't cause a source to become indirect
                    eid="e1",
                    action="enriched",
                    sourceit=("s1", "2000-01-01T00:00:00Z"),
                    sourcerefs={"a": "2"},
                    spathl=[("e20", ("a1", "1")), ("e1", ("a1", "1"))],
                ),
                gen.binary_event(
                    eid="e1",
                    sourceit=("s1", "2000-01-01T00:00:00Z"),
                    sourcerefs={"a": "1"},
                    spathl=[("e10", ("a1", "1"))],
                ),
            ]
        )
        ret = binary_summary.read(self.writer, "e1").sources
        self.assertFormatted(
            ret,
            [
                models_restapi.BinarySource(
                    source="s1",
                    direct=[
                        models_restapi.EventSource(
                            security="LOW TLP:CLEAR",
                            name="s1",
                            timestamp="2000-01-01T00:00:00Z",
                            references={"a": "2"},
                            track_source_references="s1.71f7a3fc5e701dc951de5db708d127cc",
                        )
                    ],
                    indirect=[
                        models_restapi.EventSource(
                            security="LOW TLP:CLEAR",
                            name="s1",
                            timestamp="2000-01-01T00:00:00Z",
                            references={"a": "1"},
                            track_source_references="s1.dc389854214fc40bd59a17fa2a956ea3",
                        )
                    ],
                )
            ],
        )

    def test_feature_types(self):
        self.write_plugin_events(
            [gen.plugin(authornv=("a1", "1"), features=["f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10"])]
        )

        ev1 = gen.binary_event(
            eid="e1",
            fvtl=[
                ("f1", "http://blah@tester.com", "uri"),
                ("f2", "string1", "string"),
                ("f3", "1000", "integer"),
                ("f4", "1000.01", "float"),
                ("f5", "aGVsbG8=", "binary"),
                ("f6", "2000-01-01T12:00+00:00", "datetime"),
                ("f7", "/dev/lib/opt/bad.exe", "filepath"),
                ("f8", "C:/dev/lib/opt/bad.exe", "filepath"),
                ("f9", "\\dev\\lib\\opt\\bad.exe", "filepath"),
                ("f10", "C:\\dev\\lib\\opt\\bad.exe", "filepath"),
            ],
        )
        ev1.entity.features.append(gen.feature(fv=("f11", "abacus"), patch={"offset": 123, "size": 12}))
        ev1.entity.features.append(gen.feature(fv=("f11", "abacus"), patch={"offset": 998, "size": 54}))
        self.write_binary_events([ev1])

        meta = binary_summary.read(self.writer, "e1")
        self.assertFormatted(
            meta.features,
            [
                models_restapi.BinaryFeatureValue(
                    name="f1",
                    type=azm.FeatureType.Uri,
                    value="http://blah@tester.com",
                    parts=models_restapi.FeatureValuePart(
                        scheme="http", netloc="blah@tester.com", username="blah", hostname="tester.com"
                    ),
                    instances=["e1.plugin.generic_plugin.sourced"],
                ),
                models_restapi.BinaryFeatureValue(
                    name="f10",
                    type=azm.FeatureType.Filepath,
                    value="C:\\dev\\lib\\opt\\bad.exe",
                    parts=models_restapi.FeatureValuePart(
                        filepath="C:/dev/lib/opt/bad.exe",
                        filepath_unix=["C:", "C:/dev", "C:/dev/lib", "C:/dev/lib/opt"],
                        filepath_unixr=["bad.exe", "opt/bad.exe", "lib/opt/bad.exe", "dev/lib/opt/bad.exe"],
                    ),
                    instances=["e1.plugin.generic_plugin.sourced"],
                ),
                models_restapi.BinaryFeatureValue(
                    name="f11",
                    type=azm.FeatureType.String,
                    value="abacus",
                    parts=models_restapi.FeatureValuePart(location=[[123, 135], [998, 1052]]),
                    instances=["e1.plugin.generic_plugin.sourced"],
                ),
                models_restapi.BinaryFeatureValue(
                    name="f2",
                    type=azm.FeatureType.String,
                    value="string1",
                    parts=models_restapi.FeatureValuePart(),
                    instances=["e1.plugin.generic_plugin.sourced"],
                ),
                models_restapi.BinaryFeatureValue(
                    name="f3",
                    type=azm.FeatureType.Integer,
                    value="1000",
                    parts=models_restapi.FeatureValuePart(integer="1000"),
                    instances=["e1.plugin.generic_plugin.sourced"],
                ),
                models_restapi.BinaryFeatureValue(
                    name="f4",
                    type=azm.FeatureType.Float,
                    value="1000.01",
                    parts=models_restapi.FeatureValuePart(float="1000.01"),
                    instances=["e1.plugin.generic_plugin.sourced"],
                ),
                models_restapi.BinaryFeatureValue(
                    name="f5",
                    type=azm.FeatureType.Binary,
                    value="aGVsbG8=",
                    parts=models_restapi.FeatureValuePart(binary_string="hello"),
                    instances=["e1.plugin.generic_plugin.sourced"],
                ),
                models_restapi.BinaryFeatureValue(
                    name="f6",
                    type=azm.FeatureType.Datetime,
                    value="2000-01-01T12:00+00:00",
                    parts=models_restapi.FeatureValuePart(datetime="2000-01-01T12:00+00:00"),
                    instances=["e1.plugin.generic_plugin.sourced"],
                ),
                models_restapi.BinaryFeatureValue(
                    name="f7",
                    type=azm.FeatureType.Filepath,
                    value="/dev/lib/opt/bad.exe",
                    parts=models_restapi.FeatureValuePart(
                        filepath="/dev/lib/opt/bad.exe",
                        filepath_unix=["", "/dev", "/dev/lib", "/dev/lib/opt"],
                        filepath_unixr=["bad.exe", "opt/bad.exe", "lib/opt/bad.exe", "dev/lib/opt/bad.exe"],
                    ),
                    instances=["e1.plugin.generic_plugin.sourced"],
                ),
                models_restapi.BinaryFeatureValue(
                    name="f8",
                    type=azm.FeatureType.Filepath,
                    value="C:/dev/lib/opt/bad.exe",
                    parts=models_restapi.FeatureValuePart(
                        filepath="C:/dev/lib/opt/bad.exe",
                        filepath_unix=["C:", "C:/dev", "C:/dev/lib", "C:/dev/lib/opt"],
                        filepath_unixr=["bad.exe", "opt/bad.exe", "lib/opt/bad.exe", "dev/lib/opt/bad.exe"],
                    ),
                    instances=["e1.plugin.generic_plugin.sourced"],
                ),
                models_restapi.BinaryFeatureValue(
                    name="f9",
                    type=azm.FeatureType.Filepath,
                    value="\\dev\\lib\\opt\\bad.exe",
                    parts=models_restapi.FeatureValuePart(
                        filepath="/dev/lib/opt/bad.exe",
                        filepath_unix=["", "/dev", "/dev/lib", "/dev/lib/opt"],
                        filepath_unixr=["bad.exe", "opt/bad.exe", "lib/opt/bad.exe", "dev/lib/opt/bad.exe"],
                    ),
                    instances=["e1.plugin.generic_plugin.sourced"],
                ),
            ],
        )

    def test_read_security(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a1", "1"), authorsec=gen.g1_1),
                gen.binary_event(eid="e1", authornv=("a2", "1"), authorsec=gen.g1_1),
                gen.binary_event(eid="e1", authornv=("a3", "1"), authorsec=gen.g2_1),
                gen.binary_event(eid="e1", authornv=("a4", "1"), authorsec=gen.g3_1),
                gen.binary_event(eid="e1", authornv=("a5", "1"), authorsec=gen.g1_1),
            ]
        )
        ret = binary_summary.read(self.writer, "e1").security
        self.assertFormatted(ret, ["LOW TLP:CLEAR", "MEDIUM REL:APPLE", "MEDIUM MOD1 REL:APPLE"])
        ret = binary_summary.read(self.writer, "E1").security
        self.assertFormatted(ret, ["LOW TLP:CLEAR", "MEDIUM REL:APPLE", "MEDIUM MOD1 REL:APPLE"])

    def test_binary_read_authors(self):
        self.write_binary_events(
            [gen.binary_event(eid="e1", authornv=("a1", "1"), fvl=[("f1", f"v{x}") for x in range(10)])]
        )
        ret = binary_summary.read(self.writer, "e1").instances
        self.assertFormatted(
            ret,
            [
                models_restapi.EntityInstance(
                    key="e1.plugin.a1.sourced",
                    author=models_restapi.EntityInstanceAuthor(
                        security="LOW TLP:CLEAR", category="plugin", name="a1", version="1"
                    ),
                    action="sourced",
                    num_feature_values=10,
                )
            ],
        )
        ret = binary_summary.read(self.writer, "E1").instances
        self.assertFormatted(
            ret,
            [
                models_restapi.EntityInstance(
                    key="e1.plugin.a1.sourced",
                    author=models_restapi.EntityInstanceAuthor(
                        security="LOW TLP:CLEAR", category="plugin", name="a1", version="1"
                    ),
                    action="sourced",
                    num_feature_values=10,
                )
            ],
        )

    def test_binary_read_info(self):
        self.write_binary_events([gen.binary_event(eid="e1", info={"size": "biglish"})])
        ret = binary_summary.read(self.writer, "e1").info
        self.assertFormatted(
            ret, [models_restapi.BinaryInfo(info={"size": "biglish"}, instance="e1.plugin.generic_plugin.sourced")]
        )

    def test_binary_read_author(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    fvl=[("f1", "v1")],
                    authornv=("author1", "1.2.1"),
                    authorsec="LOW TLP:CLEAR",
                )
            ]
        )
        ret = binary_summary.read(self.writer, "e1").instances
        self.assertFormatted(
            ret,
            [
                models_restapi.EntityInstance(
                    key="e1.plugin.author1.sourced",
                    author=models_restapi.EntityInstanceAuthor(
                        security="LOW TLP:CLEAR", category="plugin", name="author1", version="1.2.1"
                    ),
                    action="sourced",
                    num_feature_values=1,
                )
            ],
        )

    def test_read_parents(self):
        a = ("p1", "1")
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", spathl=[]),
                gen.binary_event(eid="e10", spathl=[("e1", a)], authorsec=gen.g2_1),
                gen.binary_event(eid="e100", spathl=[("e1", a), ("e10", a)], authorsec=gen.g3_1),
            ]
        )
        ret = binary_summary.read(self.writer, "e10").parents
        self.assertFormatted(
            ret,
            [
                models_restapi.PathNode(
                    sha256="e1",
                    action=azm.BinaryAction.Extracted,
                    timestamp="2021-01-01T12:00:00Z",
                    author=azm.Author(
                        category="plugin",
                        name="generic_plugin",
                        version="2021-01-01T12:00:00+00:00",
                        security="MEDIUM REL:APPLE",
                    ),
                    relationship={"random": "data", "action": "extracted", "label": "within"},
                    file_format_legacy="Text",
                    file_format="text/plain",
                    size=1024,
                    track_link="e1.e10.plugin.generic_plugin.2021-01-01T12:00:00+00:00",
                )
            ],
        )
        ret = binary_summary.read(self.writer, "e100").parents
        self.assertFormatted(
            ret,
            [
                models_restapi.PathNode(
                    sha256="e10",
                    action=azm.BinaryAction.Extracted,
                    timestamp="2021-01-01T12:00:00Z",
                    author=azm.Author(
                        category="plugin",
                        name="generic_plugin",
                        version="2021-01-01T12:00:00+00:00",
                        security="MEDIUM MOD1 REL:APPLE",
                    ),
                    relationship={"random": "data", "action": "extracted", "label": "within"},
                    file_format_legacy="Text",
                    file_format="text/plain",
                    size=1024,
                    track_link="e10.e100.plugin.generic_plugin.2021-01-01T12:00:00+00:00",
                )
            ],
        )
