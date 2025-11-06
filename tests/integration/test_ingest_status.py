from unittest import mock

from azul_metastore import ingestor
from azul_metastore.common.manager import Manager
from azul_metastore.common.wrapper import Wrapper
from azul_metastore.query import status
from tests.support import gen, integration_test


@mock.patch("azul_metastore.ingestor.BaseIngestor.is_done")
@mock.patch("azul_metastore.ingestor.BaseIngestor.get_data")
class TestIngestor(integration_test.DynamicTestCase):

    def test_status(self, _get_data, _done):
        tmp = _get_data.return_value = [
            gen.status(eid="e1", authorsec=gen.g1_1),
            gen.status(eid="e2", authorsec=gen.g1_1),
            gen.status(eid="e3", authorsec=gen.g1_1),
            gen.status(eid="e4", authorsec=gen.g2_1),
            gen.status(eid="e5", authorsec=gen.g3_1),
        ]
        # remove security
        tmp[1].author.security = None

        _done.side_effect = [False, True]
        ing = ingestor.StatusIngestor(self.writer)
        ing.main()
        self.flush()

        resp = status._get_opensearch_binary_status(self.writer, sha256="e1")
        self.assertEqual(1, len(resp))
        self.assertEqual("generic_plugin", resp[0].author.name)
        self.assertEqual(resp[0].entity.status, "heartbeat")

        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es1, sha256="e1")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es1, sha256="e2")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es1, sha256="e3")))
        self.assertEqual(0, len(status._get_opensearch_binary_status(self.es1, sha256="e4")))
        self.assertEqual(0, len(status._get_opensearch_binary_status(self.es1, sha256="e5")))

        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es2, sha256="e1")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es2, sha256="e2")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es2, sha256="e3")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es2, sha256="e4")))
        self.assertEqual(0, len(status._get_opensearch_binary_status(self.es2, sha256="e5")))

        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e1")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e2")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e3")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e4")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e5")))

    def test_status_duplicate_events(self, _get_data, _done):
        _get_data.return_value = [
            gen.status(eid="e1", runtime=10, timestamp="2021-01-01T12:00:00+00:00"),
            gen.status(eid="e1", runtime=20, timestamp="2021-01-01T12:00:01+00:00"),
            gen.status(eid="e1", runtime=30, timestamp="2021-01-01T12:00:02+00:00"),
        ]

        _done.side_effect = [False, True]
        ing = ingestor.StatusIngestor(self.writer)
        ing.main()
        self.flush()

        # Verify that only the last event provided is kept (the newest one) when eventid's are duplicates
        evs = status._get_opensearch_binary_status(self.es1, sha256="e1")
        self.assertEqual(1, len(evs))
        self.assertEqual(evs[0].entity.runtime, 30)

    def test_status_duplicate_events_different_order(self, _get_data, _done):
        _get_data.return_value = [
            gen.status(eid="e1", runtime=10, timestamp="2021-01-01T12:00:00+00:00"),
            gen.status(eid="e1", runtime=30, timestamp="2021-01-01T12:00:02+00:00"),
            gen.status(eid="e1", runtime=20, timestamp="2021-01-01T12:00:01+00:00"),
        ]

        _done.side_effect = [False, True]
        ing = ingestor.StatusIngestor(self.writer)
        ing.main()
        self.flush()

        # Verify that only the last event provided is kept (the newest one) when eventid's are duplicates
        evs = status._get_opensearch_binary_status(self.es1, sha256="e1")
        self.assertEqual(1, len(evs))
        self.assertEqual(evs[0].entity.runtime, 30)

    def test_create_status_with_opensearch_failures(self, _get_data, _done):
        _get_data.return_value = [
            gen.status(eid="badId" * 100000),
            gen.status(eid="goodId"),
            gen.status(eid="goodId1"),
            gen.status(eid="goodId2"),
            gen.status(eid="goodId3"),
        ]
        _done.side_effect = [False, True]
        ing = ingestor.StatusIngestor(self.writer)
        with mock.patch.object(
            Wrapper, "wrap_and_index_docs", wraps=ing.ctx.man.status.w.wrap_and_index_docs
        ) as plugin_wrapper:
            with mock.patch.object(Manager, "check_canary") as check_canary:
                check_canary.return_value = True

                ing.main()
                self.flush()
                # Should be called exactly 1 times
                self.assertEqual(1, plugin_wrapper.call_count)

        _get_data.return_value = [
            gen.status(eid="badId" * 100000),
            gen.status(eid="badId2" * 100000),
            gen.status(eid="badId3" * 100000),
            gen.status(eid="goodId2"),
            gen.status(eid="goodId3"),
        ]
        _done.side_effect = [False, True]
        ing = ingestor.StatusIngestor(self.writer)
        with mock.patch.object(
            Wrapper, "wrap_and_index_docs", wraps=ing.ctx.man.status.w.wrap_and_index_docs
        ) as plugin_wrapper:
            with mock.patch.object(Manager, "check_canary") as check_canary:
                check_canary.return_value = True

                ing.main()
                self.flush()
                # Should be called exactly 1 times
                self.assertEqual(1, plugin_wrapper.call_count)

    def test_create_status_with_pydantic_failures(self, _get_data, _done):
        tmp = [
            gen.status(eid="badId"),
            gen.status(eid="goodId"),
        ]
        tmp[0].author = ""
        tmp[1].author = ""
        _get_data.return_value = tmp
        _done.side_effect = [False, True]
        ing = ingestor.StatusIngestor(self.writer)
        with mock.patch.object(
            Wrapper, "wrap_and_index_docs", wraps=ing.ctx.man.status.w.wrap_and_index_docs
        ) as plugin_wrapper:
            with mock.patch.object(Manager, "check_canary") as check_canary:
                check_canary.return_value = True

                ing.main()
                self.flush()
                # Should be called exactly 0 times because everything should fail during pydantic validation.
                self.assertEqual(0, plugin_wrapper.call_count)

    def test_status_security_string(self, _get_data, _done):
        tmp = _get_data.return_value = [
            gen.status(eid="e1", authorsec=gen.g1_1),
            gen.status(eid="e2", authorsec=gen.g1_1),
            gen.status(eid="e3", authorsec=gen.g1_1),
            gen.status(eid="e4", authorsec=gen.g2_1),
            gen.status(eid="e5", authorsec=gen.g3_1),
        ]
        # remove security
        tmp[1].author.security = None

        _done.side_effect = [False, True]
        ing = ingestor.StatusIngestor(self.writer)
        ing.main()
        self.flush()

        resp = status._get_opensearch_binary_status(self.writer, sha256="e1")
        self.assertEqual(1, len(resp))
        self.assertEqual("generic_plugin", resp[0].author.name)
        self.assertEqual(resp[0].entity.status, "heartbeat")

        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es1, sha256="e1")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es1, sha256="e2")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es1, sha256="e3")))
        self.assertEqual(0, len(status._get_opensearch_binary_status(self.es1, sha256="e4")))
        self.assertEqual(0, len(status._get_opensearch_binary_status(self.es1, sha256="e5")))

        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es2, sha256="e1")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es2, sha256="e2")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es2, sha256="e3")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es2, sha256="e4")))
        self.assertEqual(0, len(status._get_opensearch_binary_status(self.es2, sha256="e5")))

        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e1")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e2")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e3")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e4")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e5")))
