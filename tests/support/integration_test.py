import json
import os
import time
from typing import Iterable
from unittest import mock

import pendulum
import urllib3
from azul_bedrock import models_api as mapi
from azul_bedrock import models_network as azm

from azul_metastore import context, opensearch_config, settings
from azul_metastore.common import memcache, search_data
from azul_metastore.encoders import binary2 as rc2
from azul_metastore.query import annotation, binary_create, plugin, status
from tests.support import auth, basic_test, gen, system

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def sius(key, data):
    if not os.environ.get(key):
        if os.environ.get(key.upper()):
            os.environ[key] = os.environ.get(key.upper())
        else:
            os.environ[key] = data
    return os.environ[key]


# Prevents recreation of opensearch roles each run, which is very time consuming.
# If you are testing specific files frequently, you might want to switch this to True to prevent
# the initial recreation as well.
created_opensearch_roles = False


class DynamicTestCase(basic_test.BasicTest):
    base: context.Context
    writer: context.Context

    system: system.System

    @classmethod
    def flush(cls):
        cls.system.flush()

    @classmethod
    def alter_environment(cls):
        # if these vars are unset, use defaults that work with docker-compose.yml
        sius("metastore_opensearch_username", "azul_writer")
        sius("metastore_opensearch_password", "dummyPassword!")
        sius("metastore_opensearch_url", "https://localhost:9204")
        # these user/pass variables are only used by testing code
        sius("TEST_OPENSEARCH_ELEVATED_USER", "azul_admin")
        sius("TEST_OPENSEARCH_ELEVATED_PASSWORD", "dummyPassword!")
        # End of environment variables only for tests
        os.environ["metastore_admin_check_bypass"] = "true"
        sius("metastore_certificate_verification", "false")
        cls.partition = sius("metastore_partition", "integration")
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
                    "origin": "REL:APPLE",
                    "origin_alt_name": "AGAO",
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
                        {"name": "TLP:AMBER+STRICT"},
                    ],
                },
            }
        )
        os.environ["security_default"] = "LOW"
        os.environ["security_presets"] = json.dumps(["LOW"])
        os.environ["security_minimum_required_access"] = json.dumps(["LOW"])
        os.environ["metastore_statuses_expire_events_after"] = "14 days"
        os.environ["metastore_sources"] = json.dumps(
            {
                "s1": {},
                "s2": {},
                "s3": {},
                "s4": {},
                "s5": {},
                "generic_source": {},
            }
        )
        os.environ["metastore_log_level"] = "DEBUG"
        os.environ["metastore_purge_sha256_folder"] = "/tmp/"
        os.environ["metastore_dispatcher_events_url"] = "http://localhost"
        os.environ["metastore_dispatcher_streams_url"] = "http://localhost"
        # We don't need to ingest the default 10,000 to test this:
        os.environ["metastore_warn_on_event_count"] = str(5)

    @classmethod
    def setUpClass(cls):
        global created_opensearch_roles
        cls.clear_cache()
        cls.alter_environment()
        s = settings.get()
        print(f"using opensearch at {s.opensearch_url}")

        # Update opensearch config if we have elevated permissions.
        # This must be done before using the writer user, as it creates the correct roles.
        username = os.environ["TEST_OPENSEARCH_ELEVATED_USER"]
        password = os.environ["TEST_OPENSEARCH_ELEVATED_PASSWORD"]
        if not created_opensearch_roles and username and password:
            created_opensearch_roles = True
            credentials = {
                "unique": username,
                "format": "basic",
                "username": username,
                "password": password,
            }
            admin_session = search_data.SearchData(credentials, security_exclude=[], security_include=[])
            opensearch_config.write_config_to_opensearch(admin_session, rolesmapping=True)

        # delete any existing azul indices with the writer user
        cls.system = system.System(s.partition)
        cls.system.setup(delete_existing=True)
        cls.setup_users()

        # print the whole difference between datastructures in a failed test
        cls.maxDiff = None

        cls.dp_submit_binary = mock.patch(
            "azul_bedrock.dispatcher.DispatcherAPI.submit_binary",
            side_effect=lambda x, y, z: gen.gen_binary_data(z, label=y),
        )
        cls.async_dp_submit_binary = mock.patch(
            "azul_bedrock.dispatcher.DispatcherAPI.async_submit_binary",
            side_effect=basic_test.resp_async_submit_binary,
        )
        cls.dp_submit_events = mock.patch(
            "azul_bedrock.dispatcher.DispatcherAPI.submit_events", side_effect=basic_test.resp_submit_events
        )
        cls.dp_delete_binary = mock.patch(
            "azul_bedrock.dispatcher.DispatcherAPI.delete_binary", side_effect=lambda *vs, **kv: (True, True)
        )
        cls.dp_simulate_consumers_on_event = mock.patch(
            "azul_bedrock.dispatcher.DispatcherAPI.simulate_consumers_on_event",
            side_effect=lambda *vs, **kv: (mapi.EventSimulate(consumers=[])),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        # delete all indices, important for automated testing as we use different partition for each build
        # and must ensure no leftover indices
        cls.system.delete_indexes()

    def setUp(self):
        # FUTURE make similar to unit tests
        self.dp_submit_binary_mm = self.dp_submit_binary.start()
        self.dp_submit_events_mm = self.dp_submit_events.start()
        self.dp_delete_binary_mm = self.dp_delete_binary.start()
        self.dp_simulate_consumers_on_event_mm = self.dp_simulate_consumers_on_event.start()
        self.async_dp_submit_binary_mm = self.async_dp_submit_binary.start()

        self.system.delete_all_docs()
        self.writer = self.system.writer
        self.clear_cache()
        # prevent count cache carrying between tests
        self.clear_count_cache()
        # prevent query state carrying between tests
        self.writer.clear_state()

    def tearDown(self) -> None:
        self.dp_submit_binary.stop()
        self.dp_submit_events.stop()
        self.dp_delete_binary.stop()
        self.dp_simulate_consumers_on_event.stop()
        self.async_dp_submit_binary.stop()
        # delete docs here to not influence runtime of next test
        self.system.delete_all_docs()

    def clear_count_cache(self):
        try:
            self.system.es_admin.delete_by_query(
                index=self.writer.man.cache.w.alias, refresh=True, body={"query": {"match_all": {}}}
            )
        except Exception as e:
            # version conflict workaround
            time.sleep(0.1)
            self.system.es_admin.delete_by_query(
                index=self.writer.man.cache.w.alias, refresh=True, body={"query": {"match_all": {}}}
            )

    @classmethod
    def clear_cache(cls):
        memcache.clear()

    @classmethod
    def _write_events(cls, *, fn, now, must_error, refresh: bool = True):
        # set now to the past so everything gets written unless we override manually
        if not now:
            now = pendulum.datetime(year=2000, month=1, day=1)
        with mock.patch("pendulum.now", lambda: now):
            failed, dropped_duplicates = fn()
        if refresh:
            cls.flush()
        if must_error != failed:
            raise Exception(f"expected _write_events() failures {failed=} did not match {must_error=}")

    @classmethod
    def write_plugin_events(cls, plugin_events=None, *, now=None, must_error: int = 0):
        if not plugin_events:
            plugin_events = [gen.plugin(features=["generic_feature", "f1", "f2", "f3", "f4", "f5"])]
        cls._write_events(
            fn=lambda: plugin.create_plugin(cls.system.writer, plugin_events), now=now, must_error=must_error
        )

    @classmethod
    def write_binary_events(
        cls, binary_events: list[azm.BinaryEvent | dict], *, now=None, must_error: int = 0, refresh: bool = True
    ):
        """Write binary events to the metastore."""
        cls.write_plugin_events(now=now)
        cls._write_events(
            fn=lambda: binary_create.create_binary_events(cls.system.writer, binary_events),
            now=now,
            must_error=must_error,
            refresh=refresh,
        )

    @classmethod
    def write_entity_tags(cls, tags: list[dict]):
        ret = annotation.create_binary_tags(cls.system.writer, "generic_owner", tags)
        cls.flush()
        return ret

    @classmethod
    def write_fv_tags(cls, tags: list[dict]):
        ret = annotation.create_feature_value_tags(cls.system.writer, "generic_owner", tags)
        cls.flush()
        return ret

    @classmethod
    def write_status_events(cls, statuses: list[dict], *, now=None, must_error: int = 0):
        cls._write_events(fn=lambda: status.create_status(cls.system.writer, statuses), now=now, must_error=must_error)

    @classmethod
    def setup_users(cls):
        cls.es1 = cls.system.get_ctx(auth.Auth.user_low)
        cls.es2 = cls.system.get_ctx(auth.Auth.user_med)
        cls.es3 = cls.system.get_ctx(auth.Auth.user_high)
        cls.es3o2 = cls.system.get_ctx(auth.Auth.user_high_org2)
        cls.user_high_all = cls.system.get_ctx(auth.Auth.user_high_all)

    def count_binary_events(self, _id, *, user=None):
        return len(list(self.read_binary_events(_id, user=user, raw=True)))

    def read_binary_events(self, sha256: str, *, user=None, raw: bool = False) -> list[dict]:
        """Read raw result documents - intended for debugging."""
        if not user:
            user = self.user_high_all
        sha256 = sha256.lower()
        return [x for x in self._stream_binary2_events(sha256, user=user, raw=raw)]

    def _stream_binary2_events(self, sha256: str, *, user=None, raw: bool = False) -> Iterable[dict]:
        """Read raw result documents - intended for debugging."""
        if not user:
            user = self.user_high_all
        sha256 = sha256.lower()
        body = {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"sha256": sha256}},
                        {"has_child": {"type": "metadata", "query": {"term": {"sha256": sha256}}}},
                    ]
                }
            },
        }
        for resp in user.man.binary2.w.scan(user.sd, body=body, routing=sha256):
            resp["_source"]["_index"] = resp["_index"]
            resp["_source"]["_id"] = resp["_id"]
            if raw:
                yield resp["_source"]
            else:
                if "sha256" not in resp["_source"]:
                    # skip parent docs
                    continue
                yield rc2.Binary2.decode(resp["_source"])


class BaseRestapi(DynamicTestCase):
    def get_dp_events(self) -> list[dict]:
        """Return all events that were submitted to the dispatcher since test start."""
        dp_events = []
        for args, kwargs in self.dp_submit_events_mm.call_args_list:
            dp_events += args[0]  # first arg is events list
        dp_events = self.dp_submit_events_mm.call_args_list[0][0][0]
        # for comparison must convert to dict
        encoded_events = [x.model_dump_json(exclude_defaults=True, exclude_unset=True) for x in dp_events]
        decoded_events = [json.loads(x) for x in encoded_events]

        # for easier comparison, replace with fixed timestamp
        fixed_timestamp = "2024-01-22T01:00:00+00:00"
        for event in decoded_events:
            event["timestamp"] = fixed_timestamp
            if "source" in event:
                event["source"]["path"][-1]["timestamp"] = fixed_timestamp
        return decoded_events

    def setUp(self):
        self.client = system.get_app()
        return super().setUp()
