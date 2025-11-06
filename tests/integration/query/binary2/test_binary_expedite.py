from azul_bedrock import models_network as azm

from azul_metastore.query.binary2 import binary_expedite
from tests.support import gen, integration_test


class TestSearchEntity(integration_test.DynamicTestCase):

    def test_yield_expedite_events(self):
        self.write_binary_events(
            [
                # Sourced event which should be detected.
                gen.binary_event(eid="e1"),
                # Extracted event which should be detected.
                gen.binary_event(eid="e1", authornv=("a1", "1"), action=azm.BinaryAction.Extracted),
                # Different binary should be ignored
                gen.binary_event(eid="e2"),
                # Ensure enrichment events are ignored
                gen.binary_event(eid="e1", action=azm.BinaryAction.Enriched, authornv=("a2", "1")),
                gen.binary_event(eid="e1", action=azm.BinaryAction.Enriched, authornv=("a3", "1")),
                gen.binary_event(eid="e1", action=azm.BinaryAction.Enriched, authornv=("a1", "1")),
                # Ensure enrichment various other event types are ignored
                gen.binary_event(eid="e1", action=azm.BinaryAction.Augmented, authornv=("a5", "1")),
                gen.binary_event(eid="e1", action=azm.BinaryAction.Mapped, authornv=("a6", "1")),
            ]
        )
        events = list(binary_expedite._yield_expedite_events(self.writer, "e1", False))
        self.assertEqual(2, sum(len(x) for x in events))
        self.assertTrue(True, events[0][0].flags.expedite)
        self.assertFalse(False, events[0][0].flags.bypass_cache)

        events = list(binary_expedite._yield_expedite_events(self.writer, "e1", True))
        self.assertEqual(2, sum(len(x) for x in events))
        self.assertTrue(events[0][0].flags.expedite)
        self.assertTrue(events[0][0].flags.bypass_cache)

        events = list(binary_expedite._yield_expedite_events(self.writer, "e1", False))
        # BinaryEvents
        for evt in events:
            self.assertIn(
                evt[0].action,
                [
                    azm.BinaryAction.Sourced,
                    azm.BinaryAction.Extracted,
                ],
            )
