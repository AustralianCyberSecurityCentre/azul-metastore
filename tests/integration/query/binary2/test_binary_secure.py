from azul_metastore.query import plugin
from azul_metastore.query.binary2 import binary_summary
from tests.support import gen
from tests.support import integration_test as etb


class TestEntityGroups(etb.DynamicTestCase):

    def test_access(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a1", "1"), authorsec=gen.g1_1, sourcesec=gen.g1_1),
                gen.binary_event(eid="e1", authornv=("a2", "1"), authorsec=gen.g2_1, sourcesec=gen.g2_1),
                gen.binary_event(eid="e1", authornv=("a3", "1"), authorsec=gen.g3_1, sourcesec=gen.g3_1),
                gen.binary_event(eid="e1", authornv=("a4", "1"), authorsec=gen.g1_12, sourcesec=gen.g1_12),
                gen.binary_event(eid="e1", authornv=("a5", "1"), authorsec=gen.g2_12, sourcesec=gen.g2_12),
                gen.binary_event(eid="e1", authornv=("a6", "1"), authorsec=gen.g3_12, sourcesec=gen.g3_12),
            ]
        )
        self.assertEqual(6, len(binary_summary.read(self.writer, "e1").instances))
        self.assertEqual(2, len(binary_summary.read(self.es1, "e1").instances))
        self.assertEqual(4, len(binary_summary.read(self.es2, "e1").instances))
        self.assertEqual(6, len(binary_summary.read(self.es3, "e1").instances))
        self.assertEqual(4, len(binary_summary.read(self.es3o2, "e1").instances))

        self.es3.sd.exclude = ["MEDIUM", "MOD1"]
        self.assertEqual(6, len(binary_summary.read(self.es3, "e1").instances))
        self.es2.sd.exclude = ["MEDIUM", "MOD1"]
        self.assertEqual(4, len(binary_summary.read(self.es2, "e1").instances))

    def test_minimum_access(self):
        plugin_events = [
            gen.plugin(
                features=["generic_feature", "f1", "f2", "f3", "f4", "f5"],
                authorsec="LOW",
            )
        ]
        plugin.create_plugin(self.system.writer, plugin_events)
        self.flush()
        res_open = self.es1.sd.es().count(index=self.system.index_opened)
        res_closed = self.es1.sd.es().count(index=self.system.index_closed)
        self.assertEqual(res_open["count"], 1)
        self.assertEqual(res_closed["count"], 0)
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    authornv=("a1", "1"),
                    authorsec="LOW",
                    sourcesec="LOW",
                ),
                gen.binary_event(
                    eid="e1",
                    authornv=("a2", "1"),
                    authorsec="LOW",
                    sourcesec="LOW",
                ),
                gen.binary_event(
                    eid="e1",
                    authornv=("a3", "1"),
                    authorsec="LOW",
                    sourcesec="LOW",
                ),
                gen.binary_event(
                    eid="e1",
                    authornv=("a4", "1"),
                    authorsec="LOW",
                    sourcesec="LOW",
                ),
            ],
        )
        self.flush()
        self.assertEqual(4, len(binary_summary.read(self.writer, "e1").instances))
        self.assertEqual(4, len(binary_summary.read(self.es1, "e1").instances))
        res_open = self.es1.sd.es().count(index=self.system.index_opened)
        res_closed = self.es1.sd.es().count(index=self.system.index_closed)
        self.assertEqual(res_open["count"], 1)
        self.assertEqual(res_closed["count"], 5)

    def test_access_closed(self):
        plugin_events = [
            gen.plugin(
                features=["generic_feature", "f1", "f2", "f3", "f4", "f5"],
                authorsec="MEDIUM REL:APPLE",
            )
        ]
        plugin.create_plugin(self.system.writer, plugin_events)
        self.flush()
        res_open = self.es2.sd.es().count(index=self.system.index_opened)
        res_closed = self.es2.sd.es().count(index=self.system.index_closed)
        self.assertEqual(res_open["count"], 0)
        self.assertEqual(res_closed["count"], 1)
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    authornv=("a1", "1"),
                    authorsec="MEDIUM REL:APPLE",
                    sourcesec="MEDIUM REL:APPLE",
                ),
                gen.binary_event(
                    eid="e1",
                    authornv=("a2", "1"),
                    authorsec="MEDIUM REL:APPLE",
                    sourcesec="MEDIUM REL:APPLE",
                ),
                gen.binary_event(
                    eid="e1",
                    authornv=("a3", "1"),
                    authorsec="MEDIUM REL:APPLE",
                    sourcesec="MEDIUM REL:APPLE",
                ),
                gen.binary_event(
                    eid="e1",
                    authornv=("a4", "1"),
                    authorsec="MEDIUM REL:APPLE",
                    sourcesec="MEDIUM REL:APPLE",
                ),
            ],
        )
        self.flush()
        self.assertEqual(4, len(binary_summary.read(self.writer, "e1").instances))
        self.assertEqual(4, len(binary_summary.read(self.es2, "e1").instances))
        res_open = self.es2.sd.es().count(index=self.system.index_opened)
        res_closed = self.es2.sd.es().count(index=self.system.index_closed)
        self.assertEqual(res_open["count"], 1)
        self.assertEqual(res_closed["count"], 6)

    def tearDown(self) -> None:
        self.es2.sd.exclude = []
        self.es3.sd.exclude = []
        super().tearDown()
