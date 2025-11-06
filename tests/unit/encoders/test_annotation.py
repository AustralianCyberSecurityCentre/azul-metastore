import json

from azul_metastore.encoders import annotation
from tests.support import unit_test


class TestAnnotation(unit_test.BaseUnitTestCase):
    def test_mapping(self):
        # test that the mapping is serialisable
        json.dumps(annotation.Annotation.mapping)

    def test_entity_tag_encode_trivial(self):
        raw = {
            "type": "entity_tag",
            "security": "LOW",
            "timestamp": "2010-01-01T00:00+00:00",
            "model": "binary",
            "sha256": "12345",
            "tag": "virus",
            "owner": "laundry",
        }
        annotation.Annotation.encode(raw)

    def test_entity_tag_decode_trivial(self):
        raw = {
            "type": "entity_tag",
            "security": "LOW",
            "timestamp": "2010-01-01T00:00+00:00",
            "model": "binary",
            "sha256": "12345",
            "tag": "virus",
            "owner": "laundry",
        }
        annotation.Annotation.decode(annotation.Annotation.encode(raw))

    def test_fv_tag_encode_trivial(self):
        raw = {
            "type": "fv_tag",
            "security": "LOW",
            "timestamp": "2010-01-01T00:00+00:00",
            "feature_name": "filetype",
            "feature_value": "pdf",
            "tag": "virus",
            "owner": "laundry",
        }
        annotation.Annotation.encode(raw)

    def test_fv_tag_decode_trivial(self):
        raw = {
            "type": "fv_tag",
            "security": "LOW",
            "timestamp": "2010-01-01T00:00+00:00",
            "feature_name": "filetype",
            "feature_value": "pdf",
            "tag": "virus",
            "owner": "laundry",
        }
        annotation.Annotation.decode(annotation.Annotation.encode(raw))
