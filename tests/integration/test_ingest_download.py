from unittest import mock

from opensearchpy import RequestError

from azul_metastore import ingestor
from azul_metastore.common.wrapper import Wrapper
from azul_metastore.query import status
from tests.support import gen, integration_test
from azul_bedrock import models_network as azm


@mock.patch("azul_metastore.ingestor.BaseIngestor.is_done")
@mock.patch("azul_metastore.ingestor.BaseIngestor.get_data")
class TestIngestor(integration_test.DynamicTestCase):
    def test_download(self, _get_data, _done):
        tmp = _get_data.return_value = [
            gen.download(hash="e1", authorsec=gen.g1_1),
            gen.download(hash="e1", authorsec=gen.g1_1),  # duplicate to remove
            gen.download(hash="e2", authorsec=gen.g1_1),
            gen.download(hash="e3", authorsec=gen.g2_1),
            gen.download(hash="e4", authorsec=gen.g3_1),
            gen.download(hash="e5", authorsec=gen.g3_1),
        ]
        # remove security
        tmp[2].author.security = None

        _done.side_effect = [False, True]
        ing = ingestor.DownloadIngestor(self.writer)
        ing.main()
        self.flush()

        resp = status._get_opensearch_binary_status(self.writer, sha256="e1")
        self.assertEqual(1, len(resp))
        self.assertEqual("generic_plugin", resp[0].author.name)
        self.assertEqual(resp[0].entity.status, azm.StatusEnum.DOWNLOAD_REQUESTED.value)

        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es1, sha256="e1")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es1, sha256="e2")))
        self.assertEqual(0, len(status._get_opensearch_binary_status(self.es1, sha256="e3")))
        self.assertEqual(0, len(status._get_opensearch_binary_status(self.es1, sha256="e4")))
        self.assertEqual(0, len(status._get_opensearch_binary_status(self.es1, sha256="e5")))

        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es2, sha256="e1")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es2, sha256="e2")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es2, sha256="e3")))
        self.assertEqual(0, len(status._get_opensearch_binary_status(self.es2, sha256="e4")))
        self.assertEqual(0, len(status._get_opensearch_binary_status(self.es2, sha256="e5")))

        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e1")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e2")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e3")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e4")))
        self.assertEqual(1, len(status._get_opensearch_binary_status(self.es3, sha256="e5")))

    def test_download_duplicate_events(self, _get_data, _done):
        _get_data.return_value = [
            gen.download(hash="e1", action=azm.DownloadAction.Requested, timestamp="2021-01-01T12:00:00+00:00"),
            gen.download(hash="e1", action=azm.DownloadAction.FailedNotFound, timestamp="2021-01-01T12:00:01+00:00"),
            gen.download(hash="e1", action=azm.DownloadAction.Success, timestamp="2021-01-01T12:00:02+00:00"),
        ]

        _done.side_effect = [False, True]
        ing = ingestor.DownloadIngestor(self.writer)
        ing.main()
        self.flush()

        # Verify that only the last event provided is kept (the newest one) when eventid's are duplicates
        evs = status._get_opensearch_binary_status(self.es1, sha256="e1")
        self.assertEqual(1, len(evs))
        self.assertEqual(evs[0].entity.status, azm.StatusEnum.COMPLETED.value)

    def test_download_duplicate_events_different_order(self, _get_data, _done):
        _get_data.return_value = [
            gen.download(hash="e1", action=azm.DownloadAction.Requested, timestamp="2021-01-01T12:00:00+00:00"),
            gen.download(hash="e1", action=azm.DownloadAction.Success, timestamp="2021-01-01T12:00:02+00:00"),
            gen.download(hash="e1", action=azm.DownloadAction.FailedNotFound, timestamp="2021-01-01T12:00:01+00:00"),
        ]

        _done.side_effect = [False, True]
        ing = ingestor.DownloadIngestor(self.writer)
        ing.main()
        self.flush()

        # Verify that only the last event provided is kept (the newest one) when eventid's are duplicates
        evs = status._get_opensearch_binary_status(self.es1, sha256="e1")
        self.assertEqual(1, len(evs))
        self.assertEqual(evs[0].entity.status, azm.StatusEnum.COMPLETED.value)

    def test_create_download_with_opensearch_failures(self, _get_data, _done):
        _get_data.return_value = [
            gen.download(hash="badId", authorsec="badsecurity" * 1000),
            gen.download(hash="goodId"),
            gen.download(hash="goodId1"),
            gen.download(hash="goodId2"),
            gen.download(hash="goodId3"),
        ]
        _done.side_effect = [False, True]
        ing = ingestor.DownloadIngestor(self.writer)
        with mock.patch.object(
            Wrapper, "wrap_and_index_docs", wraps=ing.ctx.man.status.w.wrap_and_index_docs
        ) as plugin_wrapper:
            ing.main()
            self.flush()
            # Should be called exactly 1 times
            self.assertEqual(1, plugin_wrapper.call_count)

        _get_data.return_value = [
            gen.download(hash="badId", authorsec="badsecurity" * 1000),
            gen.download(hash="badId2", authorsec="badsecurity" * 1000),
            gen.download(hash="badId3", authorsec="badsecurity" * 1000),
            gen.download(hash="goodId2"),
            gen.download(hash="goodId3"),
        ]
        _done.side_effect = [False, True]
        ing = ingestor.DownloadIngestor(self.writer)
        with mock.patch.object(
            Wrapper, "wrap_and_index_docs", wraps=ing.ctx.man.status.w.wrap_and_index_docs
        ) as plugin_wrapper:
            ing.main()
            self.flush()
            # Should be called exactly 1 times
            self.assertEqual(1, plugin_wrapper.call_count)

    def test_create_download_with_pydantic_failures(self, _get_data, _done):
        tmp = [
            gen.download(hash="badId"),
            gen.download(hash="goodId"),
        ]
        tmp[0].author = ""
        tmp[1].author = ""
        _get_data.return_value = tmp
        _done.side_effect = [False, True]
        ing = ingestor.DownloadIngestor(self.writer)
        with mock.patch.object(
            Wrapper, "wrap_and_index_docs", wraps=ing.ctx.man.status.w.wrap_and_index_docs
        ) as plugin_wrapper:
            ing.main()
            self.flush()
            # Should be called exactly 0 times because everything should fail during pydantic validation.
            self.assertEqual(0, plugin_wrapper.call_count)

    def test_download_status_security_string(self, _get_data, _done):
        tmp = _get_data.return_value = [
            gen.download(hash="e1", authorsec=gen.g1_1),
            gen.download(hash="e2", authorsec=gen.g1_1),
            gen.download(hash="e3", authorsec=gen.g1_1),
            gen.download(hash="e4", authorsec=gen.g2_1),
            gen.download(hash="e5", authorsec=gen.g3_1),
        ]
        # remove security
        tmp[1].author.security = None

        _done.side_effect = [False, True]
        ing = ingestor.DownloadIngestor(self.writer)
        ing.main()
        self.flush()

        resp = status._get_opensearch_binary_status(self.writer, sha256="e1")
        self.assertEqual(1, len(resp))
        self.assertEqual("generic_plugin", resp[0].author.name)
        self.assertEqual(resp[0].entity.status, azm.StatusEnum.DOWNLOAD_REQUESTED.value)

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
