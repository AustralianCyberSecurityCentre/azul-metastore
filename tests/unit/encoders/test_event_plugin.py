import json

from azul_metastore.encoders import plugin as ep
from tests.support import gen, unit_test


class TestEventPlugin(unit_test.BaseUnitTestCase):
    def test_mapping(self):
        # test that the mapping is serialisable
        json.dumps(ep.Plugin.mapping)

    def test_encode_trivial(self):
        raw = gen.plugin(model=False, authornv=("author", "1"), features=["1", "2", "3"])
        data = ep.Plugin.encode(raw)

    def test_decode_trivial(self):
        raw = gen.plugin(model=False, authornv=("author", "1"), features=["1", "2", "3"])
        data = ep.Plugin.decode(ep.Plugin.encode(raw))
