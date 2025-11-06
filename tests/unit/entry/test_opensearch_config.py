from unittest import mock

from click.testing import CliRunner

from azul_metastore import entry
from azul_metastore.opensearch_config import get_opensearch_cli_commands
from tests.support import unit_test


class TestOpensearchConfig(unit_test.BaseUnitTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_username = "test-user-1"
        self.test_password = "myAwesomeTestPassword1!"
        self.test_jwt = "wow-this-is-a-jwt-i-can-barely-believe-it!"
        self.test_oauth = "wow-this-is-an-oauth-token-thats-awesomeeeeeeeeeeeeeeeeee"

    def test_get_configs(self):
        result = get_opensearch_cli_commands(rolesmapping=False)
        print("ACTUAL\n\n", result, "\n\nEND ACTUAL")
        self.assertEqual(
            result,
            [
                'PUT _plugins/_security/api/roles/azul-fill1\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/azul-fill2\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/azul-fill3\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/azul-fill4\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/azul-fill5\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/azul_read\n{\n    "cluster_permissions": [\n        "indices:data/read/*",\n        "kibana_user"\n    ],\n    "index_permissions": [\n        {\n            "allowed_actions": [\n                "indices:admin/resolve/index"\n            ],\n            "fls": [],\n            "index_patterns": [\n                "*"\n            ],\n            "masked_fields": []\n        },\n        {\n            "allowed_actions": [\n                "indices:admin/get",\n                "indices:admin/analyze"\n            ],\n            "fls": [],\n            "index_patterns": [\n                "azul.*"\n            ],\n            "masked_fields": []\n        },\n        {\n            "allowed_actions": [\n                "read"\n            ],\n            "dls": "{\\"bool\\": {\\"filter\\": [{\\"terms\\": {\\"encoded_security.inclusive\\": [${user.securityRoles}]}},{\\"terms_set\\": {\\"encoded_security.exclusive\\": {\\"terms\\": [${user.securityRoles}], \\"minimum_should_match_field\\": \\"encoded_security.num_exclusive\\"}}},{\\"bool\\":{\\"should\\":[{\\"bool\\":{\\"must_not\\":{\\"terms\\":{\\"encoded_security.markings\\":[\\"s-tlp-amber-strict\\"]}}}},{\\"bool\\":{\\"must\\":{\\"terms\\":{\\"encoded_security.markings\\":[${user.securityRoles}]}}}}],\\"minimum_should_match\\":1}}]}}",\n            "fls": [],\n            "index_patterns": [\n                "azul.x.*"\n            ],\n            "masked_fields": []\n        },\n        {\n            "allowed_actions": [\n                "read"\n            ],\n            "fls": [],\n            "index_patterns": [\n                "azul.o.*"\n            ],\n            "masked_fields": []\n        }\n    ]\n}',
                'PUT _plugins/_security/api/roles/azul_write\n{\n    "cluster_permissions": [\n        "cluster_monitor",\n        "cluster:admin/script/*",\n        "indices:admin/index_template/*",\n        "indices:admin/mapping/*",\n        "indices:admin/template/*",\n        "indices:data/read/*",\n        "indices:data/write/bulk",\n        "kibana_user"\n    ],\n    "index_permissions": [\n        {\n            "allowed_actions": [\n                "unlimited"\n            ],\n            "fls": [],\n            "index_patterns": [\n                "azul.*"\n            ],\n            "masked_fields": []\n        }\n    ]\n}',
                'PUT _plugins/_security/api/roles/s-any\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-hanoverlap\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-high\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-low\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-low--ly\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-medium\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-mod1\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-mod2\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-mod3\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-over\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-rel-apple\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-rel-bee\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-rel-car\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-tlp-amber\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-tlp-amber-strict\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-tlp-clear\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-tlp-green\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-top-high\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
            ],
        )
        result = get_opensearch_cli_commands(rolesmapping=True)
        print("ACTUAL2\n\n", result, "\n\nEND ACTUAL2")
        self.assertEqual(
            result,
            [
                'PUT _plugins/_security/api/roles/azul-fill1\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/azul-fill2\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/azul-fill3\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/azul-fill4\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/azul-fill5\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/azul_read\n{\n    "cluster_permissions": [\n        "indices:data/read/*",\n        "kibana_user"\n    ],\n    "index_permissions": [\n        {\n            "allowed_actions": [\n                "indices:admin/resolve/index"\n            ],\n            "fls": [],\n            "index_patterns": [\n                "*"\n            ],\n            "masked_fields": []\n        },\n        {\n            "allowed_actions": [\n                "indices:admin/get",\n                "indices:admin/analyze"\n            ],\n            "fls": [],\n            "index_patterns": [\n                "azul.*"\n            ],\n            "masked_fields": []\n        },\n        {\n            "allowed_actions": [\n                "read"\n            ],\n            "dls": "{\\"bool\\": {\\"filter\\": [{\\"terms\\": {\\"encoded_security.inclusive\\": [${user.securityRoles}]}},{\\"terms_set\\": {\\"encoded_security.exclusive\\": {\\"terms\\": [${user.securityRoles}], \\"minimum_should_match_field\\": \\"encoded_security.num_exclusive\\"}}},{\\"bool\\":{\\"should\\":[{\\"bool\\":{\\"must_not\\":{\\"terms\\":{\\"encoded_security.markings\\":[\\"s-tlp-amber-strict\\"]}}}},{\\"bool\\":{\\"must\\":{\\"terms\\":{\\"encoded_security.markings\\":[${user.securityRoles}]}}}}],\\"minimum_should_match\\":1}}]}}",\n            "fls": [],\n            "index_patterns": [\n                "azul.x.*"\n            ],\n            "masked_fields": []\n        },\n        {\n            "allowed_actions": [\n                "read"\n            ],\n            "fls": [],\n            "index_patterns": [\n                "azul.o.*"\n            ],\n            "masked_fields": []\n        }\n    ]\n}',
                'PUT _plugins/_security/api/roles/azul_write\n{\n    "cluster_permissions": [\n        "cluster_monitor",\n        "cluster:admin/script/*",\n        "indices:admin/index_template/*",\n        "indices:admin/mapping/*",\n        "indices:admin/template/*",\n        "indices:data/read/*",\n        "indices:data/write/bulk",\n        "kibana_user"\n    ],\n    "index_permissions": [\n        {\n            "allowed_actions": [\n                "unlimited"\n            ],\n            "fls": [],\n            "index_patterns": [\n                "azul.*"\n            ],\n            "masked_fields": []\n        }\n    ]\n}',
                'PUT _plugins/_security/api/roles/s-any\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-hanoverlap\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-high\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-low\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-low--ly\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-medium\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-mod1\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-mod2\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-mod3\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-over\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-rel-apple\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-rel-bee\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-rel-car\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-tlp-amber\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-tlp-amber-strict\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-tlp-clear\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-tlp-green\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/roles/s-top-high\n{\n    "cluster_permissions": [],\n    "index_permissions": []\n}',
                'PUT _plugins/_security/api/rolesmapping/azul-fill1\n{\n    "backend_roles": [\n        "azul_read"\n    ],\n    "description": "Maps azul_read to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/azul-fill2\n{\n    "backend_roles": [\n        "azul_read"\n    ],\n    "description": "Maps azul_read to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/azul-fill3\n{\n    "backend_roles": [\n        "azul_read"\n    ],\n    "description": "Maps azul_read to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/azul-fill4\n{\n    "backend_roles": [\n        "azul_read"\n    ],\n    "description": "Maps azul_read to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/azul-fill5\n{\n    "backend_roles": [\n        "azul_read"\n    ],\n    "description": "Maps azul_read to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/azul_read\n{\n    "backend_roles": [\n        "azul_read"\n    ],\n    "description": "Maps azul_read to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/azul_write\n{\n    "backend_roles": [\n        "azul_write"\n    ],\n    "description": "Maps azul_write to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-any\n{\n    "backend_roles": [\n        "azul_read"\n    ],\n    "description": "Maps azul_read to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-hanoverlap\n{\n    "backend_roles": [\n        "HANOVERLAP"\n    ],\n    "description": "Maps HANOVERLAP to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-high\n{\n    "backend_roles": [\n        "HIGH"\n    ],\n    "description": "Maps HIGH to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-low\n{\n    "backend_roles": [\n        "LOW"\n    ],\n    "description": "Maps LOW to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-low--ly\n{\n    "backend_roles": [\n        "LOW: LY"\n    ],\n    "description": "Maps LOW: LY to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-medium\n{\n    "backend_roles": [\n        "MEDIUM"\n    ],\n    "description": "Maps MEDIUM to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-mod1\n{\n    "backend_roles": [\n        "MOD1"\n    ],\n    "description": "Maps MOD1 to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-mod2\n{\n    "backend_roles": [\n        "MOD2"\n    ],\n    "description": "Maps MOD2 to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-mod3\n{\n    "backend_roles": [\n        "MOD3"\n    ],\n    "description": "Maps MOD3 to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-over\n{\n    "backend_roles": [\n        "OVER"\n    ],\n    "description": "Maps OVER to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-rel-apple\n{\n    "backend_roles": [\n        "REL:APPLE"\n    ],\n    "description": "Maps REL:APPLE to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-rel-bee\n{\n    "backend_roles": [\n        "REL:BEE"\n    ],\n    "description": "Maps REL:BEE to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-rel-car\n{\n    "backend_roles": [\n        "REL:CAR"\n    ],\n    "description": "Maps REL:CAR to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-tlp-amber\n{\n    "backend_roles": [\n        "TLP:AMBER"\n    ],\n    "description": "Maps TLP:AMBER to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-tlp-amber-strict\n{\n    "backend_roles": [\n        "TLP:AMBER+STRICT"\n    ],\n    "description": "Maps TLP:AMBER+STRICT to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-tlp-clear\n{\n    "backend_roles": [\n        "TLP:CLEAR"\n    ],\n    "description": "Maps TLP:CLEAR to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-tlp-green\n{\n    "backend_roles": [\n        "TLP:GREEN"\n    ],\n    "description": "Maps TLP:GREEN to this role"\n}',
                'PUT _plugins/_security/api/rolesmapping/s-top-high\n{\n    "backend_roles": [\n        "TOP HIGH"\n    ],\n    "description": "Maps TOP HIGH to this role"\n}',
            ],
        )

    def test_get_config_through_entrypoint(self):
        runner = CliRunner()
        result = runner.invoke(entry.apply_opensearch_config, args="--print-only", catch_exceptions=False)
        print(result.output)
        # Just check that the PUT substring appears often enough that the output looks reasonably correct.
        self.assertEqual(result.output.count("PUT _plugins/_security/api/roles"), 25)

    @mock.patch("azul_metastore.common.search_data.SearchData")
    @mock.patch(
        "azul_metastore.opensearch_config.write_config_to_opensearch",
    )
    def test_provide_jwt(self, write_config_mock: mock.MagicMock, _sd):
        runner = CliRunner()
        result = runner.invoke(
            entry.apply_opensearch_config,
            input=f"y\n{entry.AuthOptions.user_and_password.value}\n{self.test_username}\n{self.test_password}",
            catch_exceptions=False,
        )
        print(result.output)
        self.assertIn("Successfully created and validated all roles.", result.output)

    @mock.patch("azul_metastore.common.search_data.SearchData")
    @mock.patch(
        "azul_metastore.opensearch_config.write_config_to_opensearch",
    )
    def test_provide_oauth(self, write_config_mock: mock.MagicMock, _sd):
        runner = CliRunner()
        result = runner.invoke(
            entry.apply_opensearch_config,
            input=f"y\n{entry.AuthOptions.oauth_token.value}\n{self.test_oauth}",
            catch_exceptions=False,
        )
        print(result.output)
        self.assertIn("Successfully created and validated all roles.", result.output)

        result = runner.invoke(
            entry.apply_opensearch_config,
            ["--rolesmapping"],
            input=f"y\n{entry.AuthOptions.oauth_token.value}\n{self.test_oauth}",
            catch_exceptions=False,
        )
        print(result.output)
        self.assertIn("Additionally creating role mappings.", result.output)
        self.assertIn("Successfully created and validated all roles.", result.output)

    @mock.patch("azul_metastore.common.search_data.SearchData")
    @mock.patch(
        "azul_metastore.opensearch_config.write_config_to_opensearch",
    )
    def test_provide_basic_creds(self, write_config_mock: mock.MagicMock, _sd):
        runner = CliRunner()
        result = runner.invoke(
            entry.apply_opensearch_config,
            input=f"y\n{entry.AuthOptions.jwt.value}\n{self.test_jwt}",
            catch_exceptions=False,
        )
        print(result.output)
        self.assertIn("Successfully created and validated all roles.", result.output)
