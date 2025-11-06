import datetime

from azul_bedrock import models_restapi

from azul_metastore.query import plugin, status
from tests.support import gen, integration_test


class TestPlugin(integration_test.DynamicTestCase):
    def test_plugin(self):
        config = {"server": '"http://localhost"', "heartbeat_interval": "30"}
        bad_docs_count, duplicates_count = plugin.create_plugin(
            self.writer,
            [
                gen.plugin(authornv=("p1", "1"), config=config, features=["f1"]),
                gen.plugin(authornv=("p2", "1"), config=config, features=["f1", "f2"]),
                gen.plugin(authornv=("p3", "2"), config=config),
                gen.plugin(authornv=("p4", "1"), config=config),
                gen.plugin(authornv=("p4", "1"), config=config),
                gen.plugin(authornv=("p4", "1"), config=config),
                gen.plugin(authornv=("p5", "1"), config={"myconfig": '"'}),  # non-json
            ],
        )
        # ensure that non-json data is rejected
        self.assertEqual(1, bad_docs_count)
        # Verify there are 2 duplicates in the count.
        self.assertEqual(2, duplicates_count)
        bad_docs_count, duplicates_count = plugin.create_plugin(
            self.writer,
            [
                gen.plugin(authornv=("p3", "2"), config=config),
            ],
        )
        self.assertEqual(0, bad_docs_count)
        self.assertEqual(0, duplicates_count)

        resp = plugin.get_plugin(self.writer, "p1", "1")
        self.assertEqual(config, resp["config"])
        self.assertEqual(1, len(resp["features"]))

        resp = plugin.get_plugin(self.writer, "p2", "1")
        self.assertEqual(config, resp["config"])
        self.assertEqual(2, len(resp["features"]))

        resp = plugin.get_plugin(self.writer, "p3", "1")
        self.assertEqual(None, resp)

        resp = plugin.get_plugin(self.writer, "p3", "2")
        self.assertEqual(config, resp["config"])

    def test_read_plugins(self):
        config = {"server": '"http://localhost"', "heartbeat_interval": "30"}
        plugin.create_plugin(
            self.writer,
            [
                gen.plugin(authornv=("p1", "1"), config=config, features=["f1"]),
                gen.plugin(authornv=("p2", "1"), config=config, features=["f1", "f2"]),
                gen.plugin(authornv=("p3", "2"), config=config, features=[]),
                gen.plugin(authornv=("p4", "1"), config=config, features=[]),
            ],
        )

        resp = plugin.get_all_plugins(self.writer)
        self.assertEqual(4, len(resp))
        self.assertEqual("p1", resp[0].newest_version.name)
        self.assertEqual("1", resp[0].newest_version.version)
        self.assertEqual(1, len(resp[0].newest_version.features))
        self.assertEqual("p2", resp[1].newest_version.name)
        self.assertEqual("1", resp[1].newest_version.version)
        self.assertEqual(2, len(resp[1].newest_version.features))
        self.assertEqual("p3", resp[2].newest_version.name)
        self.assertEqual("2", resp[2].newest_version.version)
        self.assertIsNone(resp[2].newest_version.features)

        resp = plugin.get_all_plugins_config(self.writer)
        self.assertFormatted(
            resp,
            {
                "p1": {"server": "http://localhost", "heartbeat_interval": 30},
                "p2": {"server": "http://localhost", "heartbeat_interval": 30},
                "p3": {"server": "http://localhost", "heartbeat_interval": 30},
                "p4": {"server": "http://localhost", "heartbeat_interval": 30},
            },
        )

    def test_simple(self):
        self.write_plugin_events(plugin_events=[gen.plugin(features=["f1", "f2", "f3", "f4", "f5", "f6"])])

        features = plugin.find_features(self.writer)
        self.assertEqual(6, len(features))
        f1 = [x for x in features if x.name == "f1"][0]
        self.assertEqual("f1", f1.name)
        self.assertEqual("generic_plugin", f1.descriptions[0].author_name)

    def test_descriptions(self):
        self.write_plugin_events(
            plugin_events=[
                gen.plugin(authornv=("a1", "1"), features=["f1"], authorsec=gen.g1_1),
                gen.plugin(authornv=("a2", "1"), features=["f1"], authorsec=gen.g2_1),
                gen.plugin(authornv=("a3", "1"), features=["f1"], authorsec=gen.g3_1),
            ]
        )
        descs = plugin.find_features(self.es1)
        f1 = [x for x in descs if x.name == "f1"][0]
        self.assertEqual(1, len(f1.descriptions))

        descs = plugin.find_features(self.es2)
        f1 = [x for x in descs if x.name == "f1"][0]
        self.assertEqual(2, len(f1.descriptions))

        descs = plugin.find_features(self.es3)
        f1 = [x for x in descs if x.name == "f1"][0]
        self.assertEqual(3, len(f1.descriptions))

    def test_get_author_stats(self):
        now = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        eightDaysAgo = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=8)
        eightDaysAgo = eightDaysAgo.isoformat()
        status.create_status(
            self.writer,
            [
                gen.status(eid="e1", authornv=("a1", "1"), status="completed", ts=now),
                gen.status(eid="e2", authornv=("a1", "1"), status="opt-out", ts=now),
                gen.status(eid="e3", authornv=("a1", "1"), status="error-exception", ts=now),
                gen.status(eid="e4", authornv=("a1", "1"), status="error-timeout", ts=now),
                gen.status(eid="e5", authornv=("a1", "1"), status="error-output", ts=now),
                gen.status(eid="e6", authornv=("a1", "1"), status="error-input", ts=now),
                gen.status(eid="e7", authornv=("a1", "1"), status="heartbeat", ts=now),
                # Old statuses that will be filtered out.
                gen.status(eid="e8", authornv=("a1", "1"), status="completed", ts=eightDaysAgo),
                gen.status(eid="e9", authornv=("a1", "1"), status="completed", ts="2000-01-01T00:00:00Z"),
            ],
        )
        self.flush()

        stats = plugin.get_author_stats(self.writer, "a1", "1")
        for sg in stats:
            for item in sg.items:
                item.timestamp = now
        self.assertFormatted(
            stats,
            [
                models_restapi.StatusGroup(
                    status="completed",
                    num_items=1,
                    items=[
                        models_restapi.StatusEvent(
                            timestamp=now,
                            author=models_restapi.Author(
                                security="LOW TLP:CLEAR", category="plugin", name="a1", version="1"
                            ),
                            entity=models_restapi.StatusEntity(
                                status="completed",
                                runtime=10.0,
                                input=models_restapi.StatusInput(
                                    entity=models_restapi.StatusInputEntity(sha256="e1"),
                                ),
                            ),
                            security="LOW TLP:CLEAR",
                        )
                    ],
                ),
                models_restapi.StatusGroup(
                    status="error-exception",
                    num_items=1,
                    items=[
                        models_restapi.StatusEvent(
                            timestamp=now,
                            author=models_restapi.Author(
                                security="LOW TLP:CLEAR", category="plugin", name="a1", version="1"
                            ),
                            entity=models_restapi.StatusEntity(
                                status="error-exception",
                                runtime=10.0,
                                input=models_restapi.StatusInput(
                                    entity=models_restapi.StatusInputEntity(sha256="e3"),
                                ),
                            ),
                            security="LOW TLP:CLEAR",
                        )
                    ],
                ),
                models_restapi.StatusGroup(
                    status="error-input",
                    num_items=1,
                    items=[
                        models_restapi.StatusEvent(
                            timestamp=now,
                            author=models_restapi.Author(
                                security="LOW TLP:CLEAR", category="plugin", name="a1", version="1"
                            ),
                            entity=models_restapi.StatusEntity(
                                status="error-input",
                                runtime=10.0,
                                input=models_restapi.StatusInput(
                                    entity=models_restapi.StatusInputEntity(sha256="e6"),
                                ),
                            ),
                            security="LOW TLP:CLEAR",
                        )
                    ],
                ),
                models_restapi.StatusGroup(
                    status="error-output",
                    num_items=1,
                    items=[
                        models_restapi.StatusEvent(
                            timestamp=now,
                            author=models_restapi.Author(
                                security="LOW TLP:CLEAR", category="plugin", name="a1", version="1"
                            ),
                            entity=models_restapi.StatusEntity(
                                status="error-output",
                                runtime=10.0,
                                input=models_restapi.StatusInput(
                                    entity=models_restapi.StatusInputEntity(sha256="e5"),
                                ),
                            ),
                            security="LOW TLP:CLEAR",
                        )
                    ],
                ),
                models_restapi.StatusGroup(
                    status="error-timeout",
                    num_items=1,
                    items=[
                        models_restapi.StatusEvent(
                            timestamp=now,
                            author=models_restapi.Author(
                                security="LOW TLP:CLEAR", category="plugin", name="a1", version="1"
                            ),
                            entity=models_restapi.StatusEntity(
                                status="error-timeout",
                                runtime=10.0,
                                input=models_restapi.StatusInput(
                                    entity=models_restapi.StatusInputEntity(sha256="e4"),
                                ),
                            ),
                            security="LOW TLP:CLEAR",
                        )
                    ],
                ),
                models_restapi.StatusGroup(
                    status="heartbeat",
                    num_items=1,
                    items=[
                        models_restapi.StatusEvent(
                            timestamp=now,
                            author=models_restapi.Author(
                                security="LOW TLP:CLEAR", category="plugin", name="a1", version="1"
                            ),
                            entity=models_restapi.StatusEntity(
                                status="heartbeat",
                                runtime=10.0,
                                input=models_restapi.StatusInput(
                                    entity=models_restapi.StatusInputEntity(sha256="e7"),
                                ),
                            ),
                            security="LOW TLP:CLEAR",
                        )
                    ],
                ),
                models_restapi.StatusGroup(
                    status="opt-out",
                    num_items=1,
                    items=[
                        models_restapi.StatusEvent(
                            timestamp=now,
                            author=models_restapi.Author(
                                security="LOW TLP:CLEAR", category="plugin", name="a1", version="1"
                            ),
                            entity=models_restapi.StatusEntity(
                                status="opt-out",
                                runtime=10.0,
                                input=models_restapi.StatusInput(
                                    entity=models_restapi.StatusInputEntity(sha256="e2"),
                                ),
                            ),
                            security="LOW TLP:CLEAR",
                        )
                    ],
                ),
            ],
        )
