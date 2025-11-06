from azul_metastore.query.binary2 import binary_find_paginate
from tests.support import gen, integration_test


class TestEntityFind(integration_test.DynamicTestCase):

    def test_read_entities_quick(self):
        self.write_binary_events(
            [gen.binary_event(eid=f"e{x}", authornv=("a1", "1"), fvl=[("f1", "v1")]) for x in range(100)]
            + [gen.binary_event(eid=f"e{x}", authornv=("a1", "1"), fvl=[("f1", "v2")]) for x in range(100, 200)]
        )

        all_binaries = []
        after = None
        expected = None
        while True:
            resp = binary_find_paginate.find_all_binaries(self.writer, after=after, num_binaries=100)
            if not resp.items:
                break
            if resp.total:
                expected = resp.total
            after = resp.after
            all_binaries.extend(resp.items)
        self.assertEqual(200, len(all_binaries))
        self.assertEqual(expected, 200)  # count should be accurate for <1000

        all_binaries = []
        after = None
        expected = None
        while True:
            resp = binary_find_paginate.find_all_binaries(
                self.writer, after=after, num_binaries=100, term='features_map.f1:"v1"'
            )
            if not resp.items:
                break
            if resp.total:
                expected = resp.total
            after = resp.after
            all_binaries.extend(resp.items)
        self.assertEqual(100, len(all_binaries))
        self.assertEqual(expected, 100)  # count should be accurate for <1000
