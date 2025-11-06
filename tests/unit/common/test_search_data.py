from unittest import mock

from azul_metastore.common import memcache, search_data
from tests.support import unit_test


class TestUtil(unit_test.BaseUnitTestCase):
    def test_credentials_to_access(self):
        self.assertRaises(search_data.BadCredentialsException, search_data.credentials_to_access, {"random": "stuff"})
        self.assertRaises(search_data.BadCredentialsException, search_data.credentials_to_access, {"format": "stuff"})
        self.assertRaises(search_data.BadCredentialsException, search_data.credentials_to_access, {"format": "basic"})
        self.assertRaises(search_data.BadCredentialsException, search_data.credentials_to_access, {"format": "jwt"})
        self.assertRaises(search_data.BadCredentialsException, search_data.credentials_to_access, {"format": "oauth"})

        c = search_data.credentials_to_access({"format": "basic", "username": "user", "password": "pass"})
        self.assertEqual(("user", "pass"), c["http_auth"])

        c = search_data.credentials_to_access({"format": "jwt", "token": "tok"})
        self.assertEqual("tok", c["headers"]["Authorization"])

        c = search_data.credentials_to_access({"format": "oauth", "token": "password"})
        self.assertEqual("Bearer password", c["headers"]["Authorization"])

    @mock.patch("opensearchpy.OpenSearch")
    @mock.patch("azul_metastore.common.search_data.credentials_to_access")
    def test_credentials_to_es(self, _cta, _es):
        _cta.side_effect = search_data.BadCredentialsException()
        memcache.clear()
        self.assertRaises(search_data.BadCredentialsException, search_data.credentials_to_es, {"unique": "blah"})

        _cta.side_effect = None
        _cta.return_value = {"http_auth": ("user", "pass")}
        search_data.credentials_to_es({"unique": "blah"})


class TestSearchData(unit_test.BaseUnitTestCase):
    @mock.patch("azul_metastore.common.search_data.credentials_to_es")
    def test_es(self, _cte):
        _cte.return_value = 515
        self.assertEqual(515, search_data.SearchData({}, []).es())

    @mock.patch("azul_metastore.common.search_data.credentials_to_access")
    def test_access(self, _cta):
        _cta.return_value = 515
        self.assertEqual(515, search_data.SearchData({}, []).access())

    def test_unique(self):
        self.assertEqual("cactus|", search_data.SearchData({"unique": "cactus"}, []).unique())
        self.assertEqual("cactus|a", search_data.SearchData({"unique": "cactus"}, ["a"]).unique())
        self.assertEqual("cactus|a.c", search_data.SearchData({"unique": "cactus"}, ["a", "c"]).unique())
        self.assertEqual("cactus|a.c", search_data.SearchData({"unique": "cactus"}, ["c", "a"]).unique())
