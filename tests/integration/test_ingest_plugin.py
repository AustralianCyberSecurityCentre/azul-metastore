from unittest import mock

from azul_metastore import ingestor
from azul_metastore.common.manager import Manager
from azul_metastore.common.wrapper import Wrapper
from azul_metastore.query import plugin
from tests.support import gen, integration_test


@mock.patch("azul_metastore.ingestor.BaseIngestor.is_done")
@mock.patch("azul_metastore.ingestor.BaseIngestor.get_data")
class TestIngestor(integration_test.DynamicTestCase):

    def test_plugin(self, _get_data, _done):
        tmp = _get_data.return_value = [
            gen.plugin(authornv=("a1", "1"), authorsec=gen.g1_1, config={"c1": "100"}),
            gen.plugin(authornv=("a2", "1"), authorsec=gen.g1_1),
            gen.plugin(authornv=("a3", "1"), authorsec=gen.g1_1, features=["f3"]),
            gen.plugin(authornv=("a4", "1"), authorsec=gen.g2_1, features=["f4"]),
            gen.plugin(authornv=("a5", "1"), authorsec=gen.g3_1, features=["f5"]),
        ]
        # remove attributes
        tmp[1].entity.config = {}
        tmp[1].author.security = None

        _done.side_effect = [False, True]
        ing = ingestor.PluginIngestor(self.writer)
        ing.main()
        self.flush()

        res = plugin.find_features(self.writer)
        self.assertEqual(4, len(res))
        feats = {x.name: y for x in res for y in x.descriptions}
        self.assertEqual(feats["generic_feature"].desc, "generic_description")

        res = plugin.get_plugin(self.writer, "a1", "1")
        feats = {x["name"]: x for x in res["features"]}
        self.assertEqual("100", res["config"]["c1"])
        self.assertEqual(feats["generic_feature"]["desc"], "generic_description")

        self.assertEqual(2, len(plugin.find_features(self.es1)))
        self.assertEqual(3, len(plugin.find_features(self.es2)))
        self.assertEqual(4, len(plugin.find_features(self.es3)))

        self.assertTrue(plugin.get_plugin(self.es1, "a1", "1"))
        self.assertTrue(plugin.get_plugin(self.es1, "a2", "1"))
        self.assertTrue(plugin.get_plugin(self.es1, "a3", "1"))
        self.assertFalse(plugin.get_plugin(self.es1, "a4", "1"))
        self.assertFalse(plugin.get_plugin(self.es1, "a5", "1"))

        self.assertTrue(plugin.get_plugin(self.es2, "a1", "1"))
        self.assertTrue(plugin.get_plugin(self.es2, "a2", "1"))
        self.assertTrue(plugin.get_plugin(self.es2, "a3", "1"))
        self.assertTrue(plugin.get_plugin(self.es2, "a4", "1"))
        self.assertFalse(plugin.get_plugin(self.es2, "a5", "1"))

        self.assertTrue(plugin.get_plugin(self.es3, "a1", "1"))
        self.assertTrue(plugin.get_plugin(self.es3, "a2", "1"))
        self.assertTrue(plugin.get_plugin(self.es3, "a3", "1"))
        self.assertTrue(plugin.get_plugin(self.es3, "a4", "1"))
        self.assertTrue(plugin.get_plugin(self.es3, "a5", "1"))

        self.assertFalse(plugin.get_plugin(self.es1, "invalid1", "1"))

    def test_duplicate_ingestion(self, _get_data, _done):
        _get_data.return_value = [
            gen.plugin(
                authornv=("a1", "1"),
                authorsec=gen.g1_1,
                config={"c1": "100"},
                timestamp="2021-01-01T12:00:00+00:00",
            ),
            gen.plugin(
                authornv=("a1", "1"),
                authorsec=gen.g1_1,
                config={"c1": "200"},
                timestamp="2021-01-01T12:00:01+00:00",
            ),
            gen.plugin(
                authornv=("a1", "1"),
                authorsec=gen.g3_1,
                config={"c1": "300"},
                timestamp="2021-01-01T12:00:02+00:00",
            ),
        ]

        _done.side_effect = [False, True]
        ing = ingestor.PluginIngestor(self.writer)
        ing.main()
        self.flush()

        # Verify that only the last event provided is kept (the newest one) when eventid's are duplicates
        res = plugin.get_plugin(self.writer, "a1", "1")
        print(res)
        self.assertEqual(res["config"]["c1"], "300")

    def test_duplicate_ingestion_different_order(self, _get_data, _done):
        tmp = _get_data.return_value = [
            gen.plugin(
                authornv=("a1", "1"),
                authorsec=gen.g1_1,
                config={"c1": "100"},
                timestamp="2021-01-01T12:00:00+00:00",
            ),
            gen.plugin(
                authornv=("a1", "1"),
                authorsec=gen.g3_1,
                config={"c1": "300"},
                timestamp="2021-01-01T12:00:02+00:00",
            ),
            gen.plugin(
                authornv=("a1", "1"),
                authorsec=gen.g1_1,
                config={"c1": "200"},
                timestamp="2021-01-01T12:00:01+00:00",
            ),
        ]

        _done.side_effect = [False, True]
        ing = ingestor.PluginIngestor(self.writer)
        ing.main()
        self.flush()

        # Verify that only the last event provided is kept (the newest one) when eventid's are duplicates
        res = plugin.get_plugin(self.writer, "a1", "1")
        print(res)
        self.assertEqual(res["config"]["c1"], "300")

    def test_invalid_plugin_indexing(self, _get_data, _done):
        plugin_name = "a1"
        plugin_version = "1"
        tmp = _get_data.return_value = [
            gen.plugin(
                authornv=(plugin_name, plugin_version),
                config={"thing": "a1" * 100000},
                authorsec=gen.g1_1,
                features=["f3"],
            ),
        ]
        _done.side_effect = [False, True]
        ing = ingestor.PluginIngestor(self.writer)
        ing.main()
        self.flush()

        res = plugin.get_plugin(self.writer, plugin_name, plugin_version)

    def test_create_plugin_with_opensearch_failures(self, _get_data, _done):
        plugin_name = "a1"
        plugin_version = "1"
        _get_data.return_value = [
            gen.plugin(authornv=(plugin_name, plugin_version), config={"thing": "a1" * 100000}),
            gen.plugin(authornv=("reasonable", "1")),
            gen.plugin(authornv=("reasonable", "2")),
            gen.plugin(authornv=("reasonable", "3")),
            gen.plugin(authornv=("reasonable", "4")),
        ]
        _done.side_effect = [False, True]
        ing = ingestor.PluginIngestor(self.writer)
        with mock.patch.object(
            Wrapper, "wrap_and_index_docs", wraps=ing.ctx.man.plugin.w.wrap_and_index_docs
        ) as plugin_wrapper:
            with mock.patch.object(Manager, "check_canary") as check_canary:
                check_canary.return_value = True

                ing.main()
                self.flush()
                # Should be called exactly 1 times
                self.assertEqual(1, plugin_wrapper.call_count)

    def test_create_plugin_with_pydantic_failures(self, _get_data, _done):
        tmp = [
            gen.plugin(authornv=("reasonable", "1")),
            gen.plugin(authornv=("reasonable", "2")),
        ]
        tmp[0].author = ""
        tmp[1].author = ""
        _get_data.return_value = tmp
        _done.side_effect = [False, True]
        ing = ingestor.PluginIngestor(self.writer)
        with mock.patch.object(
            Wrapper, "wrap_and_index_docs", wraps=ing.ctx.man.plugin.w.wrap_and_index_docs
        ) as plugin_wrapper:
            with mock.patch.object(Manager, "check_canary") as check_canary:
                check_canary.return_value = True

                ing.main()
                self.flush()
                # Should be called exactly 0 times because everything should fail during pydantic validation.
                self.assertEqual(0, plugin_wrapper.call_count)

    def test_plugin_security_string(self, _get_data, _done):
        tmp = _get_data.return_value = [
            gen.plugin(authornv=("a1", "1"), authorsec=gen.g1_1, config={"c1": "100"}),
            gen.plugin(authornv=("a2", "1"), authorsec=gen.g1_1),
            gen.plugin(authornv=("a3", "1"), authorsec=gen.g1_1, features=["f3"]),
            gen.plugin(authornv=("a4", "1"), authorsec=gen.g2_1, features=["f4"]),
            gen.plugin(authornv=("a5", "1"), authorsec=gen.g3_1, features=["f5"]),
        ]
        # remove attributes
        tmp[1].entity.config = {}
        tmp[1].author.security = None

        _done.side_effect = [False, True]
        ing = ingestor.PluginIngestor(self.writer)
        ing.main()
        self.flush()

        res = plugin.find_features(self.writer)
        self.assertEqual(4, len(res))
        feats = {x.name: y for x in res for y in x.descriptions}
        self.assertEqual(feats["generic_feature"].desc, "generic_description")

        res = plugin.get_plugin(self.writer, "a1", "1")
        feats = {x["name"]: x for x in res["features"]}
        self.assertEqual("100", res["config"]["c1"])
        self.assertEqual(feats["generic_feature"]["desc"], "generic_description")

        self.assertEqual(2, len(plugin.find_features(self.es1)))
        self.assertEqual(3, len(plugin.find_features(self.es2)))
        self.assertEqual(4, len(plugin.find_features(self.es3)))

        self.assertTrue(plugin.get_plugin(self.es1, "a1", "1"))
        self.assertTrue(plugin.get_plugin(self.es1, "a2", "1"))
        self.assertTrue(plugin.get_plugin(self.es1, "a3", "1"))
        self.assertFalse(plugin.get_plugin(self.es1, "a4", "1"))
        self.assertFalse(plugin.get_plugin(self.es1, "a5", "1"))

        self.assertTrue(plugin.get_plugin(self.es2, "a1", "1"))
        self.assertTrue(plugin.get_plugin(self.es2, "a2", "1"))
        self.assertTrue(plugin.get_plugin(self.es2, "a3", "1"))
        self.assertTrue(plugin.get_plugin(self.es2, "a4", "1"))
        self.assertFalse(plugin.get_plugin(self.es2, "a5", "1"))

        self.assertTrue(plugin.get_plugin(self.es3, "a1", "1"))
        self.assertTrue(plugin.get_plugin(self.es3, "a2", "1"))
        self.assertTrue(plugin.get_plugin(self.es3, "a3", "1"))
        self.assertTrue(plugin.get_plugin(self.es3, "a4", "1"))
        self.assertTrue(plugin.get_plugin(self.es3, "a5", "1"))

        self.assertFalse(plugin.get_plugin(self.es1, "invalid1", "1"))
