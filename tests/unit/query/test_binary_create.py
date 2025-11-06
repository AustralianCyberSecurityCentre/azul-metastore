from unittest import mock

from azul_metastore.common.query_info import IngestError
from azul_metastore.query import binary_create
from tests.support import gen, unit_test


class TestBinaryCreateEvents(unit_test.BaseUnitTestCase):
    @mock.patch("azul_metastore.query.binary_create._already_aged_off")
    def test_binary_create_events(self, mock_filter_age_off):
        """Test that when def create_binary_events gets a non strict_dynamic_mapping_exception and has raise_on_errors
        set to true.
        An exception of type opensearchpy.helpers.errors.BulkIndexError is raised.
        """
        self.ctx.man = mock.MagicMock()
        doc_errors = [
            IngestError(
                doc=gen.plugin(),
                error_type="not_strict_dyno_mapping_yay",
                error_reason="no reason",
            )
        ]
        self.ctx.man.binary2.w.index_docs.return_value = []
        self.ctx.man.binary2.w.index_docs.return_value = doc_errors

        mock_filter_age_off.side_effect = lambda x: False

        failures, duplicate_results = binary_create._create_binary_events(
            self.ctx,
            [
                gen.binary_event(
                    eid="e1",
                    authornv=("a1", "1"),
                    authorsec="LOW",
                    sourcesec="LOW",
                )
            ],
        )
        print(failures)
        self.assertEqual(len(failures), 1)
        self.assertEqual(len(duplicate_results), 0)
        self.assertEqual(failures[0].error_type, "not_strict_dyno_mapping_yay")
        self.assertEqual(failures[0].error_reason, "no reason")
