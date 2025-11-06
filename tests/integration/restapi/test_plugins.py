import datetime

from azul_bedrock import models_network as azm
from azul_bedrock import models_restapi

from tests.support import gen, integration_test


class TestPlugins(integration_test.BaseRestapi):
    def test_plugins_get_all_status(self):
        self.write_plugin_events([gen.plugin(authornv=("a1", "1"))])
        self.write_binary_events([gen.binary_event(authornv=("a1", "1"))])
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        yesterday = yesterday.replace(tzinfo=datetime.timezone.utc)
        yesterday_ts = yesterday.isoformat(timespec="milliseconds")
        self.write_status_events(
            [
                gen.status(eid=f"{x}f1", authornv=("a1", "1"), status=azm.StatusEnum.COMPLETED.value, ts=yesterday_ts)
                for x in range(100)
            ]
        )
        self.write_status_events(
            [
                gen.status(
                    eid=f"{x}f2", authornv=("a1", "1"), status=azm.StatusEnum.ERROR_EXCEPTION.value, ts=yesterday_ts
                )
                for x in range(50)
            ]
        )
        response = self.client.get("/v0/plugins/status")
        self.assertEqual(200, response.status_code)
        # Check last_completion corresponds to the timestamp on completed status messages
        resp = response.json()["data"]
        self.assertEqual(yesterday_ts.replace("+00:00", "Z"), resp[0]["last_completion"])
        # Check number of completed and error messages in stats
        self.assertEqual(100, resp[0]["success_count"])
        self.assertEqual(50, resp[0]["error_count"])
        resp = response.json()

    def test_plugins_get_with_multiple_versions(self):
        self.write_plugin_events(
            [
                gen.plugin({"timestamp": "2020-01-01T12:00+00:00"}, authornv=("a1", "1")),
                gen.plugin({"timestamp": "2021-01-01T12:00+00:00"}, authornv=("a1", "1.1")),
                gen.plugin({"timestamp": "2019-01-01T12:00+00:00"}, authornv=("a1", "1.2")),
                gen.plugin({"timestamp": "2016-01-01T12:00+00:00"}, authornv=("a1", "1.3")),
                gen.plugin({"timestamp": "2017-01-01T12:00+00:00"}, authornv=("a1", "1.4")),
            ]
        )
        self.write_status_events(
            [
                gen.status(eid=f"{x}f1", authornv=("a1", f"1.{x}"), status=azm.StatusEnum.COMPLETED.value)
                for x in range(5)
            ]
        )
        response = self.client.get("/v0/plugins")
        self.assertEqual(200, response.status_code)
        # Check response contains all versions of plugin
        resp = response.json()["data"]
        print(resp)
        self.assertEqual(resp[0]["versions"], ["1.1", "1", "1.2", "1.4", "1.3"])
        parsed_plugin_entity = models_restapi.PluginEntity.model_validate(resp[0]["newest_version"])
        self.assertEqual(parsed_plugin_entity.category, "plugin")
        self.assertEqual(parsed_plugin_entity.name, "a1")
        self.assertEqual(parsed_plugin_entity.version, "1.1")

    def test_plugin_get(self):
        self.write_plugin_events([gen.plugin(authornv=("a1", "1"))])
        self.write_plugin_events([gen.plugin(authornv=("a2", "1"))])
        self.write_binary_events([gen.binary_event(authornv=("a1", "1"))])
        now = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        self.write_status_events(
            [
                gen.status(eid=f"{x}f1", authornv=("a1", "1"), ts=now, status=azm.StatusEnum.COMPLETED.value)
                for x in range(112)
            ]
        )
        self.write_status_events(
            [
                gen.status(eid=f"{x}f2", authornv=("a1", "1"), ts=now, status=azm.StatusEnum.ERROR_EXCEPTION.value)
                for x in range(143)
            ]
        )
        response = self.client.get("/v0/plugins/a1/versions/1")
        self.assertEqual(200, response.status_code)
        resp = response.json()["data"]
        self.assertEqual(1, resp["num_entities"])
        self.assertEqual(azm.StatusEnum.COMPLETED.value, resp["status"][0]["status"])
        self.assertEqual(112, resp["status"][0]["num_items"])
        self.assertEqual(100, len(resp["status"][0]["items"]))
        self.assertFormatted(
            resp["status"][0]["items"][0],
            {
                "timestamp": now,
                "author": {"security": "LOW TLP:CLEAR", "category": "plugin", "name": "a1", "version": "1"},
                "entity": {
                    "status": "completed",
                    "runtime": 10.0,
                    "input": {
                        "entity": {"sha256": "0f1"},
                    },
                },
                "security": "LOW TLP:CLEAR",
            },
        )

        self.assertEqual(azm.StatusEnum.ERROR_EXCEPTION.value, resp["status"][1]["status"])
        self.assertEqual(143, resp["status"][1]["num_items"])
        self.assertEqual(100, len(resp["status"][1]["items"]))
        self.assertFormatted(
            resp["status"][1]["items"][0],
            {
                "timestamp": now,
                "entity": {
                    "status": "error-exception",
                    "runtime": 10.0,
                    "input": {
                        "entity": {"sha256": "0f2"},
                    },
                },
                "author": {"security": "LOW TLP:CLEAR", "category": "plugin", "name": "a1", "version": "1"},
                "security": "LOW TLP:CLEAR",
            },
        )

        response = self.client.get("/v0/plugins/a2/versions/1")
        self.assertEqual(200, response.status_code)
        resp = response.json()["data"]
        self.assertEqual(0, resp["num_entities"])
        self.assertEqual([], resp["status"])

        response = self.client.get("/v0/plugins/a1/versions/invalid1")
        self.assertEqual(404, response.status_code)
        resp = response.json()

        response = self.client.get("/v0/plugins/invalid1/versions/1")
        self.assertEqual(404, response.status_code)
        resp = response.json()
