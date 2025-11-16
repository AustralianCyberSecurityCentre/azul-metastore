import os

from click.testing import CliRunner

from azul_metastore import entry
from azul_metastore.common import search_data
from azul_metastore.opensearch_config import write_config_to_opensearch
from tests.support import integration_test


class TestSearch(integration_test.DynamicTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.username = os.environ["TEST_OPENSEARCH_ELEVATED_USER"]
        cls.password = os.environ["TEST_OPENSEARCH_ELEVATED_PASSWORD"]
        cls.credentials = {
            "unique": cls.username,
            "format": "basic",
            "username": cls.username,
            "password": cls.password,
        }
        cls.admin_session = search_data.SearchData(cls.credentials, security_exclude=[], security_include=[])
        cls.test_role_name = "s-low"

    def setUp(self):
        super().setUp()
        # Delete the test-role if it already exists.
        self.admin_session.es().security.delete_role(role=self.test_role_name, ignore=[404])
        val = self.admin_session.es().security.get_role(role=self.test_role_name, ignore=[404])
        # Check that the role doesn't exist.
        self.assertEqual(val["status"], "NOT_FOUND")

    @classmethod
    def tearDownClass(cls) -> None:
        # restore roles
        write_config_to_opensearch(cls.admin_session, rolesmapping=True)
        return super().tearDownClass()

    def test_create_role(self):
        write_config_to_opensearch(self.admin_session, rolesmapping=True)
        val = self.admin_session.es().security.get_role(role=self.test_role_name, ignore=[404])
        # Check role exists
        expected = {
            self.test_role_name: {
                "reserved": False,
                "hidden": False,
                "cluster_permissions": [],
                "index_permissions": [],
                "tenant_permissions": [],
                "static": False,
            }
        }
        print(val)
        self.assertEqual(val, expected)

        # Check role still exists after re-creation exists.
        write_config_to_opensearch(self.admin_session, rolesmapping=True)
        val = self.admin_session.es().security.get_role(role=self.test_role_name, ignore=[404])
        self.assertEqual(val, expected)

    def test_command_line_create_role(self):
        runner = CliRunner()
        output_text = runner.invoke(
            entry.apply_opensearch_config,
            args=["--rolesmapping"],
            input=f"y\n{entry.AuthOptions.user_and_password.value}\n{self.username}\n{self.password}\n",
            catch_exceptions=False,
        )
        print(output_text.output)

        val = self.admin_session.es().security.get_role(role=self.test_role_name, ignore=[404])
        # Check role exists
        print(val)
        self.assertEqual(
            val,
            {
                self.test_role_name: {
                    "reserved": False,
                    "hidden": False,
                    "cluster_permissions": [],
                    "index_permissions": [],
                    "tenant_permissions": [],
                    "static": False,
                }
            },
        )

    def test_command_line_create_role_assume_yes(self):
        runner = CliRunner()
        # Set env vars to test auto-selection
        os.environ["METASTORE_OPENSEARCH_ADMIN_USERNAME"] = self.username
        os.environ["METASTORE_OPENSEARCH_ADMIN_PASSWORD"] = self.password

        output_text = runner.invoke(
            entry.apply_opensearch_config,
            args=["--rolesmapping", "--no-input"],
            catch_exceptions=False,
        )
        print(output_text.output)

        val = self.admin_session.es().security.get_role(role=self.test_role_name, ignore=[404])
        # Check role exists
        print(val)
        self.assertEqual(
            val,
            {
                self.test_role_name: {
                    "reserved": False,
                    "hidden": False,
                    "cluster_permissions": [],
                    "index_permissions": [],
                    "tenant_permissions": [],
                    "static": False,
                }
            },
        )

        # Clean up env vars
        del os.environ["METASTORE_OPENSEARCH_ADMIN_USERNAME"]
        del os.environ["METASTORE_OPENSEARCH_ADMIN_PASSWORD"]
