import json

from azul_metastore.encoders import status as se
from tests.support import gen, unit_test


class TestEventStatus(unit_test.BaseUnitTestCase):
    def test_mapping(self):
        # test that the mapping is serialisable
        json.dumps(se.Status.mapping)

    def test_encode_trivial(self):
        status = gen.status(
            model=False,
            status="dequeued",
            eid="8bd74619b9c8fe6d5a586e54d10f45499fff70a4b4dfc47eef4c516646274676",
            authornv=("Plugin1", "1"),
            ts="2021-04-02T16:00+00:00",
        )
        data = se.Status.encode(status)

    def test_decode_trivial(self):
        status = gen.status(
            model=False,
            status="dequeued",
            eid="8bd74619b9c8fe6d5a586e54d10f45499fff70a4b4dfc47eef4c516646274676",
            authornv=("Plugin1", "1"),
            ts="2021-04-02T16:00+00:00",
        )
        data = se.Status.decode(se.Status.encode(status))
