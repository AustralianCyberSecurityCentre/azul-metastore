from azul_metastore.query.binary2 import binary_feature
from tests.support import gen, integration_test


class TestSearchEntity(integration_test.DynamicTestCase):

    def test_features_entity_count(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a1", "v1"), fvl=[("f1", "v1")]),
            ]
        )
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a2", "v1"), fvl=[("f2", "v1")]),
            ]
        )
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a3", "v1"), fvl=[("f2", "v3")]),
                gen.binary_event(eid="e2", authornv=("a3", "v1"), fvl=[("f2", "v3")]),
                gen.binary_event(eid="e3", authornv=("a3", "v1"), fvl=[("f2", "v3")]),
            ]
        )
        fs = binary_feature.count_binaries_with_feature_names(self.writer, ["f1"])[0]
        self.assertEqual(1, fs.entities)
        fs = binary_feature.count_binaries_with_feature_names(self.writer, ["f2"])[0]
        self.assertEqual(3, fs.entities)
