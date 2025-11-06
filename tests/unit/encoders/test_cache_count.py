import json

from azul_metastore.encoders import cache as cc
from tests.support import unit_test


class TestCacheCount(unit_test.BaseUnitTestCase):
    def test_mapping(self):
        # test that the mapping is serialisable
        json.dumps(cc.Cache.mapping)

    def test_encode_trivial(self):
        raw = {
            "security": "LOW",
            "timestamp": "2010-01-01T00:00+00:00",
            "type": "shape",
            "unique": "square",
            "docs": 123,
            "count": 11,
            "accurate": False,
            "user_security": "",
        }
        data = cc.Cache.encode(raw)

    def test_decode_trivial(self):
        raw = {
            "security": "LOW",
            "timestamp": "2010-01-01T00:00+00:00",
            "type": "shape",
            "unique": "square",
            "docs": 123,
            "count": 11,
            "accurate": False,
            "user_security": "",
        }
        data = cc.Cache.decode(cc.Cache.encode(raw))
