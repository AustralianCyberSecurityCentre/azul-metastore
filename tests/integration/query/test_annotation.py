from azul_bedrock import models_restapi

from azul_metastore.query import annotation
from azul_metastore.query.binary2 import binary_summary
from tests.support import gen
from tests.support import integration_test as etb


class TestAnnotationsRead(etb.DynamicTestCase):
    def test_init_bug(self):
        # check if there is an issue reading before writing (index creation bug)
        ret = annotation.read_all_binary_tags(self.writer)
        self.assertEqual(0, ret.num_tags)


class TestAnnotations(etb.DynamicTestCase):
    def test_value_tag_legacy(self):
        self.write_binary_events([gen.binary_event(eid="e1", features=[gen.feature(fv=("f1", "v1"))])])
        annotation.create_feature_value_tags(
            self.writer, "generic_owner", [gen.feature_value_tag(fv=("f1", "v1"), tag="t1")]
        )
        self.flush()

        def convert_bin_feat_val_to_read_feat_val_val(
            x: models_restapi.BinaryFeatureValue,
        ) -> models_restapi.ReadFeatureValuesValue:
            """Test method to simplify getting the feature values for testing."""
            dumped_model = x.model_dump(include={"name": True, "value": True})
            # irrelevant for this test but needed in the model.
            dumped_model["newest_processed"] = "2021-01-01T11:00:00Z"
            return models_restapi.ReadFeatureValuesValue.model_validate(dumped_model)

        resp = binary_summary.read(self.writer, "e1").features
        annotation.add_feature_value_tags_legacy(
            self.writer,
            [convert_bin_feat_val_to_read_feat_val_val(x) for x in resp],
        )

        self.assertEqual(len(resp[0].tags), 1)
        self.assertEqual(resp[0].tags[0].tag, "t1")

        resp = binary_summary.read(self.writer, "e1").features
        annotation.add_feature_value_tags_legacy(
            self.writer,
            [convert_bin_feat_val_to_read_feat_val_val(x) for x in resp],
        )
        self.assertEqual(len(resp[0].tags), 1)
        self.assertEqual(resp[0].tags[0].tag, "t1")

        annotation.delete_feature_value_tag(self.writer, "f1", "v1", "t1")
        self.flush()
        resp = binary_summary.read(self.writer, "e1").features
        annotation.add_feature_value_tags_legacy(
            self.writer,
            [convert_bin_feat_val_to_read_feat_val_val(x) for x in resp],
        )
        self.assertEqual(len(resp[0].tags), 0)

    def test_casing(self):
        tag = gen.entity_tag(eid="E1", tag="tag1")
        annotation.create_binary_tags(self.writer, "generic_owner", [tag])
        self.flush()

        resp = annotation.read_binary_tags(self.es1, "e1")
        self.assertEqual(1, len(resp))
        self.assertEqual("tag1", resp[0].tag)

    def test_entity_tag(self):
        tag = gen.entity_tag(eid="e1", tag="tag1")
        annotation.create_binary_tags(self.writer, "generic_owner", [tag])
        self.flush()

        resp = annotation.read_binary_tags(self.es1, "e1")
        self.assertEqual(1, len(resp))
        self.assertEqual("tag1", resp[0].tag)
        self.assertEqual("LOW TLP:CLEAR", resp[0].security)

        ret = annotation.read_all_binary_tags(self.writer)
        self.assertEqual(1, ret.num_tags)
        self.assertEqual("tag1", ret.tags[0].tag)

        annotation.delete_binary_tag(self.writer, "e1", "tag1")
        self.flush()
        resp = annotation.read_binary_tags(self.es1, "e1")
        self.assertEqual(0, len(resp))

    def test_entity_tag_no_security(self):
        tag = gen.entity_tag(eid="e1", tag="tag1")
        tag.pop("security")
        annotation.create_binary_tags(self.writer, "generic_owner", [tag])
        self.flush()

        resp = annotation.read_binary_tags(self.es1, "e1")
        self.assertEqual(1, len(resp))
        self.assertEqual("tag1", resp[0].tag)
        self.assertEqual("LOW", resp[0].security)

    def test_timestamped_feature_value_tag(self):
        self.write_binary_events([gen.binary_event(eid="e1", fvl=[("f1", "v1")])])
        annotation.create_feature_value_tags(
            self.writer,
            "generic_user",
            [{"feature_name": "f1", "feature_value": "v1", "tag": "t1", "timestamp": "2000-01-01T00:00:00Z"}],
        )
        self.flush()
        resp = binary_summary.read(self.writer, "e1").features
        annotation.add_feature_value_tags(self.writer, resp)
        feat = [x for x in resp if x.name == "f1" and x.value == "v1"][0]
        self.assertEqual(len(feat.tags), 1)
        self.assertEqual(feat.tags[0].tag, "t1")

        ret = annotation.read_all_feature_value_tags(self.writer)
        self.assertEqual(1, ret.num_tags)
        self.assertEqual("t1", ret.tags[0].tag)

        ret = annotation.read_feature_values_for_tag(self.writer, "t1")
        self.assertEqual(1, len(ret.items))
        self.assertEqual("generic_user", ret.items[0].owner)
        self.assertEqual("v1", ret.items[0].feature_value)
