from azul_metastore.query.binary2 import binary_submit_dataless
from tests.support import gen, integration_test


class TestBinaryRead(integration_test.DynamicTestCase):

    def test_stream_dispatcher(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a1", "1")),
                gen.binary_event(eid="e1", authornv=("a2", "1"), datas=[]),
                gen.binary_event(eid="e1", authornv=("a3", "1"), datas=[]),
                gen.binary_event(eid="e1", authornv=("a4", "1"), datas=[]),
                gen.binary_event(eid="e1", authornv=("a5", "1"), datas=[]),
            ]
        )
        resp = binary_submit_dataless.stream_dispatcher_events_for_binary(self.writer, "e1")
        items = [x for x in resp]
        self.assertEqual(1, len(items))
        resp = binary_submit_dataless.stream_dispatcher_events_for_binary(self.writer, "E1")
        items = [x for x in resp]
        self.assertEqual(1, len(items))
