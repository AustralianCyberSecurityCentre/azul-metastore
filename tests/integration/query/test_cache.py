from azul_metastore.query import cache
from tests.support import integration_test


class TestCache(integration_test.DynamicTestCase):
    def test_canary(self):
        # check to see if written multiple times causes issues
        self.writer.man.check_canary(self.writer.sd)
        self.writer.man.check_canary(self.writer.sd)
        self.flush()
        self.writer.man.check_canary(self.writer.sd)
        self.writer.man.check_canary(self.writer.sd)
        self.writer.man.check_canary(self.writer.sd)

    def test_data(self):
        self.writer.man.check_canary(self.writer.sd)

        cache.store_generic(self.writer, "test", "1", "v1", {"key": "doc1"})
        cache.store_generic(self.writer, "test", "2", "v1", {"key": "doc2"})
        cache.store_generic(self.writer, "test", "3", "v1", {"key": "doc3"})
        cache.store_generic(self.writer, "test", "1", "v1", {"key": "doc1.1"})

        self.assertEqual("doc1.1", cache.load_generic(self.writer, "test", "1", "v1")["key"])
        self.assertEqual("doc2", cache.load_generic(self.writer, "test", "2", "v1")["key"])
        self.assertEqual("doc3", cache.load_generic(self.writer, "test", "3", "v1")["key"])

        self.assertFalse(cache.load_generic(self.writer, "test", "invalid", "v1"))
        self.assertFalse(cache.load_generic(self.writer, "invalid", "1", "v1"))

        self.assertFalse(cache.load_generic(self.es1, "test", "1", "v1"))
        self.assertFalse(cache.load_generic(self.es2, "test", "1", "v1"))
        self.assertFalse(cache.load_generic(self.es3, "test", "1", "v1"))

        cache.store_generic(self.es1, "test", "es1", "v1", {"key": "doc1"})
        cache.store_generic(self.es2, "test", "es2", "v1", {"key": "doc1"})
        cache.store_generic(self.es3, "test", "es3", "v1", {"key": "doc1"})

        self.assertTrue(cache.load_generic(self.es1, "test", "es1", "v1"))
        self.assertFalse(cache.load_generic(self.es1, "test", "es2", "v1"))
        self.assertFalse(cache.load_generic(self.es1, "test", "es3", "v1"))

        self.assertFalse(cache.load_generic(self.es2, "test", "es1", "v1"))
        self.assertTrue(cache.load_generic(self.es2, "test", "es2", "v1"))
        self.assertFalse(cache.load_generic(self.es2, "test", "es3", "v1"))

        self.assertFalse(cache.load_generic(self.es3, "test", "es1", "v1"))
        self.assertFalse(cache.load_generic(self.es3, "test", "es2", "v1"))
        self.assertTrue(cache.load_generic(self.es3, "test", "es3", "v1"))

        # change version and check can't read
        self.assertFalse(cache.load_generic(self.es1, "test", "es1", "v2"))
