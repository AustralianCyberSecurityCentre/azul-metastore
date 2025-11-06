from azul_bedrock import models_network as azm

from azul_metastore.query.binary2 import binary_event
from tests.support import gen, integration_test


class TestBinaryRead(integration_test.DynamicTestCase):

    def test_read_best_event_for_entity(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    spathl=[("e99", ("p1", "1")), ("e1", ("p1", "1"))],
                    action=azm.BinaryAction.Enriched,
                    authornv=("p1", "1"),
                ),
                gen.binary_event(
                    eid="e1", spathl=[("e99", ("p1", "1"))], action=azm.BinaryAction.Extracted, authornv=("p2", "1")
                ),
                gen.binary_event(
                    eid="e1",
                    spathl=[("e99", ("p1", "1")), ("e1", ("p1", "1"))],
                    action=azm.BinaryAction.Augmented,
                    authornv=("p3", "1"),
                ),
                gen.binary_event(
                    eid="e2",
                    spathl=[("e99", ("p1", "1")), ("e1", ("p1", "1"))],
                    action=azm.BinaryAction.Enriched,
                    authornv=("p4", "1"),
                ),
                gen.binary_event(eid="e2", spathl=[], action=azm.BinaryAction.Sourced, authornv=("p5", "1")),
                gen.binary_event(
                    eid="e2",
                    spathl=[("e99", ("p1", "1")), ("e1", ("p1", "1"))],
                    action=azm.BinaryAction.Augmented,
                    authornv=("p6", "1"),
                ),
            ]
        )
        self.assertFormatted(binary_event.get_best_event(self.writer, "e1").action, azm.BinaryAction.Extracted)
        self.assertFormatted(binary_event.get_best_event(self.writer, "e2").action, azm.BinaryAction.Sourced)
