from unittest import mock

from azul_metastore.common import memcache, search_data
from tests.support import unit_test
from azul_bedrock import exceptions_metastore
from azul_bedrock.exception_enums import ExceptionCodeEnum
from azul_bedrock.datastore import Credentials, CredentialFormat


class TestUtil(unit_test.BaseUnitTestCase):
    @mock.patch("opensearchpy.OpenSearch")
    @mock.patch("azul_metastore.common.search_data.credentials_to_es")
    def test_credentials_to_es(self, _cta, _es):
        _cta.side_effect = exceptions_metastore.BadCredentialsException(
            internal=ExceptionCodeEnum.MetastoreOpensearchCantGetUserAccount
        )
        memcache.clear()
        self.assertRaises(
            exceptions_metastore.BadCredentialsException,
            search_data.credentials_to_es,
            Credentials(unique="cactus", format=CredentialFormat.jwt),
        )

        _cta.side_effect = None
        _cta.return_value = {"http_auth": ("user", "pass")}
        search_data.credentials_to_es({"unique": "blah"})


class TestSearchData(unit_test.BaseUnitTestCase):
    @mock.patch("azul_metastore.common.search_data.credentials_to_es")
    def test_es(self, _cte):
        _cte.return_value = 515
        self.assertEqual(
            515, search_data.SearchData(Credentials(unique="cactus", format=CredentialFormat.none), [], []).es()
        )

    @mock.patch("azul_metastore.common.search_data.bed_credentials_to_access")
    def test_access(self, _cta):
        _cta.return_value = 515
        self.assertEqual(
            515, search_data.SearchData(Credentials(unique="cactus", format=CredentialFormat.none), [], []).access()
        )

    def test_unique(self):
        self.assertEqual(
            "cactus|",
            search_data.SearchData(Credentials(unique="cactus", format=CredentialFormat.none), [], []).unique(),
        )
        self.assertEqual(
            "cactus|a",
            search_data.SearchData(Credentials(unique="cactus", format=CredentialFormat.none), ["a"], []).unique(),
        )
        self.assertEqual(
            "cactus|a.c",
            search_data.SearchData(
                Credentials(unique="cactus", format=CredentialFormat.none), ["a", "c"], []
            ).unique(),
        )
        self.assertEqual(
            "cactus|a.c",
            search_data.SearchData(
                Credentials(unique="cactus", format=CredentialFormat.none), ["c", "a"], []
            ).unique(),
        )
