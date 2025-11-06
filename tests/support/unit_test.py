import json
import os
from unittest import mock

from azul_bedrock import dispatcher
from azul_security import security

from azul_metastore.common import memcache
from azul_metastore.context import Context
from tests.support import basic_test, gen

from . import system


class BaseUnitTestCase(basic_test.BasicTest):
    maxDiff = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.client = system.get_app()

    @classmethod
    def alter_environment(cls):
        os.environ["metastore_log_level"] = "DEBUG"
        os.environ["metastore_sources"] = json.dumps(
            {
                "generic_source": {},
            }
        )
        os.environ["security_allow_releasability_priority_gte"] = "30"
        os.environ["security_labels"] = json.dumps(
            {
                "classification": {
                    "title": "Classification",
                    "options": [
                        {"name": "LOW", "priority": "10"},
                        {"name": "LOW: LY", "priority": "20"},
                        {"name": "MEDIUM", "priority": "30"},
                        {"name": "HIGH", "priority": "40"},
                        {"name": "TOP HIGH", "priority": "50"},
                    ],
                },
                "caveat": {
                    "title": "Caveat",
                    "options": [
                        {"name": "MOD1"},
                        {"name": "MOD2"},
                        {"name": "MOD3"},
                        {"name": "HANOVERLAP"},
                        {"name": "OVER"},
                    ],
                },
                "releasability": {
                    "title": "Releasability",
                    "options": [
                        {"name": "REL:APPLE"},
                        {"name": "REL:BEE"},
                        {"name": "REL:CAR"},
                    ],
                },
                "tlp": {
                    "title": "TLP",
                    "options": [
                        {"name": "TLP:CLEAR"},
                        {"name": "TLP:GREEN"},
                        {"name": "TLP:AMBER"},
                        {"name": "TLP:AMBER+STRICT", "enforce_security": "true"},
                    ],
                },
            }
        )
        os.environ["security_default"] = "LOW"
        os.environ["security_presets"] = json.dumps(["LOW"])
        os.environ["security_minimum_required_access"] = json.dumps(["LOW"])

    def setUp(self) -> None:
        memcache.clear()
        self.alter_environment()
        self.ctx = Context(
            azsec=security.Security(),
            dispatcher=dispatcher.DispatcherAPI(
                events_url="dummy",
                data_url="dummy-data",
                retry_count=3,
                timeout=15,
                author_name="mstest",
                author_version="1",
                deployment_key="",
            ),
            man=None,
        )


class FakeDispatcherAPI(dispatcher.DispatcherAPI):
    """Class used to override the internal client of DispatcherAPI to one that is always successful (200 status)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        mock_client = mock.MagicMock()
        mock_sucess = mock.MagicMock(status_code=200)
        mock_client.get.return_value = mock_sucess
        mock_client.head.return_value = mock_sucess
        mock_client.post.return_value = mock_sucess
        self._client = mock_client
        self._async_client = mock_client


class DataMockingUnitTest(BaseUnitTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        fake_dispatcher = FakeDispatcherAPI(
            events_url="dummy",
            data_url="dummy-data",
            retry_count=3,
            timeout=15,
            author_name="mstest",
            author_version="1",
            deployment_key="",
        )
        mock_get_writer_context = mock.MagicMock()
        mock_get_writer_context.dispatcher = fake_dispatcher

        # FUTURE make dp mocks easier to understand and put in base class
        cls.patches = [
            mock.patch(
                "azul_metastore.query.binary2.binary_read.find_stream_references",
                lambda *args: (True, "source", "label"),
            ),
            mock.patch("azul_metastore.context.get_writer_context", lambda *args: mock_get_writer_context),
            mock.patch("azul_metastore.query.binary_create.create_binary_events", lambda *args, **kwargs: True),
            mock.patch("azul_metastore.settings.check_source_exists", lambda *args: True),
            mock.patch(
                "azul_metastore.query.binary2.binary_read.list_all_sources_for_binary", lambda *args: ["source"]
            ),
            mock.patch("azul_metastore.settings.check_source_references", lambda *args: True),
            # mock.patch("azul_metastore.query.binary_create.append_manual_insert", lambda *args: True),
            mock.patch(
                "azul_bedrock.dispatcher.DispatcherAPI.submit_binary",
                lambda cls, x, y, z, timeout=None: gen.gen_binary_data(z, label=y),
            ),
            mock.patch(
                "azul_bedrock.dispatcher.DispatcherAPI.async_submit_binary",
                side_effect=basic_test.resp_async_submit_binary,
            ),
            mock.patch(
                "azul_bedrock.dispatcher.DispatcherAPI.submit_events", side_effect=basic_test.resp_submit_events
            ),
        ]

    def setUp(self) -> None:
        [x.start() for x in self.patches]
        super().setUp()
        memcache.clear()
        self.end = "http://localhost"
        os.environ["METASTORE_DISPATCHER_EVENTS_URL"] = self.end
        os.environ["METASTORE_DISPATCHER_STREAMS_URL"] = self.end
        os.environ["METASTORE_OPENSEARCH_URL"] = self.end
        os.environ["METASTORE_NO_SECURITY_PLUGIN_COMPATIBILITY"] = "TRUE"

    def tearDown(self) -> None:
        [x.stop() for x in self.patches]
        return super().tearDown()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
