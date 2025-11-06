from azul_bedrock import models_restapi

from azul_metastore.query.binary2 import binary_feature, binary_summary
from tests.support import gen, integration_test


class TestSearchEntity(integration_test.DynamicTestCase):

    def test_define_many_features(self):
        # create a plugin defining 2000 features
        self.write_plugin_events(
            plugin_events=[gen.plugin(authornv=("many1", "1"), features=[f"f{x}" for x in range(2000)])]
        )
        self.write_binary_events(
            [gen.binary_event(eid="e1", features=[gen.feature(fv=(f"f{x}", f"v{x}")) for x in range(2000)])]
        )

        # test some random things
        f1 = binary_feature.count_binaries_with_feature_names(self.writer, ["f1"])
        self.assertFormatted(f1, [models_restapi.FeatureMulticountRet(name="f1", entities=1)])
        f1 = binary_feature.count_values_in_features(self.writer, ["f1"])
        self.assertFormatted(f1, [models_restapi.FeatureMulticountRet(name="f1", values=1)])

        ret = binary_summary.read(self.writer, "e1").features
        self.assertEqual(2000, len(ret))

    def test_many_features(self):
        tmp = gen.binary_event(eid="e2")
        tmp.entity.features = []
        self.write_binary_events(
            [tmp, gen.binary_event(eid="e1", features=[gen.feature(fv=("f1", f"v{x}")) for x in range(20)])]
        )

        f1 = binary_feature.count_binaries_with_feature_names(self.writer, ["f1"])
        self.assertFormatted(f1, [models_restapi.FeatureMulticountRet(name="f1", entities=1)])
        f1 = binary_feature.count_values_in_features(self.writer, ["f1"])
        self.assertFormatted(f1, [models_restapi.FeatureMulticountRet(name="f1", values=20)])

        ret = binary_summary.read(self.writer, "e1").features
        self.assertEqual(20, len(ret))

        ret = binary_summary.read(self.writer, "e2").features
        self.assertEqual(0, len(ret))

    def test_too_many(self):
        self.write_binary_events(
            [gen.binary_event(eid="e1", features=[gen.feature(fv=("f1", f"v{x}")) for x in range(10001)])],
            must_error=1,
        )

        f1 = binary_feature.count_binaries_with_feature_names(self.writer, ["f1"])
        self.assertFormatted(f1, [models_restapi.FeatureMulticountRet(name="f1", entities=0)])
        f1 = binary_feature.count_values_in_features(self.writer, ["f1"])
        self.assertFormatted(f1, [models_restapi.FeatureMulticountRet(name="f1", values=0)])

        ret = binary_summary.read(self.writer, "e1").features
        self.assertEqual(0, len(ret))
