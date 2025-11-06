import datetime
import time

from azul_bedrock import models_api as mapi
from azul_bedrock import models_restapi

from azul_metastore.query import plugin, status
from tests.support import gen, integration_test


class TestStatus(integration_test.DynamicTestCase):
    def test_find(self):
        now = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        status.create_status(
            self.writer,
            [
                gen.status(eid="e1", authornv=("p1", "1"), status="dequeued", ts="2000-01-01T00:00:00Z"),
                gen.status(eid="e1", authornv=("p2", "1"), status="error-exception", ts="2000-01-01T00:00:00Z"),
                gen.status(eid="e1", authornv=("p3", "1"), status="heartbeat", ts=now),
                gen.status(eid="e1", authornv=("p4", "1"), status="completed", ts=now),
            ],
        )
        self.flush()
        statuses = status._get_opensearch_binary_status(self.writer, sha256="e1")
        self.assertEqual(4, len(statuses))
        self.assertEqual({"p1", "p2", "p3", "p4"}, {x.author.name for x in statuses})
        self.assertEqual(
            {"completed", "dequeued", "error-exception", "heartbeat"}, {x.entity.status for x in statuses}
        )

    def test_heartbeat(self):
        now = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

        status.create_status(
            self.writer,
            [
                gen.status(eid="e1", authornv=("p1", "1"), status="heartbeat", ts="2000-01-01T00:00:00Z"),
                gen.status(eid="e1", authornv=("p2", "1"), status="error-exception", ts="2000-01-01T00:00:00Z"),
                gen.status(eid="e1", authornv=("p3", "1"), status="heartbeat", ts=now),
                gen.status(eid="e1", authornv=("p4", "1"), status="heartbeat", ts=now),
            ],
        )
        plugin.create_plugin(
            self.writer,
            [
                gen.plugin(authornv=("p1", "1"), config={"heartbeat_interval": "100"}),
                gen.plugin(authornv=("p2", "1"), config={"heartbeat_interval": "100"}),
                gen.plugin(authornv=("p3", "1"), config={"heartbeat_interval": "100"}),
                gen.plugin(authornv=("p4", "1"), config={"heartbeat_interval": "100"}),
            ],
        )
        self.flush()

        # cause status to be too old
        time.sleep(2)
        results = status.get_binary_status(self.writer, "e1")

        self.assertEqual(4, len(results))
        p1 = next(result for result in results if result.author.name == "p1")
        self.assertEqual("heartbeat-lost", p1.entity.status)
        p2 = next(result for result in results if result.author.name == "p2")
        self.assertIsNone(p2.entity.error)

        # ensure other statuses don't get timed-out
        self.assertEqual(2, len([s for s in results if s.entity.status == "heartbeat"]))

    def test_heartbeat_overwrite(self):
        status.create_status(
            self.writer, [gen.status(eid="e1", authornv=("p1", "1"), status="heartbeat", ts="2000-01-01T00:00:00Z")]
        )
        self.flush()
        status.create_status(
            self.writer, [gen.status(eid="e1", authornv=("p1", "1"), status="heartbeat", ts="2000-01-02T00:00:00Z")]
        )
        self.flush()
        actual = status.get_statuses(self.writer, sha256="e1")
        self.assertEqual(1, len(actual))

    def test_duplicate_status_creation(self):
        errors, duplicate_count = status.create_status(
            self.writer,
            [
                gen.status(eid="e1", authornv=("p1", "1"), status="heartbeat", ts="2000-01-01T00:00:00Z"),
                gen.status(eid="e1", authornv=("p1", "1"), status="heartbeat", ts="2000-01-01T01:00:00Z"),
                gen.status(eid="e1", authornv=("p1", "1"), status="heartbeat", ts="2000-01-01T01:00:00Z"),
            ],
        )
        # Should be 2 duplicate statuses.
        self.assertEqual(duplicate_count, 2)

    def test_find_all(self):
        # Generate Statuses
        statuses = [
            gen.status(eid="e1", authornv=("a1", "1"), authorsec=gen.g1_1),
            gen.status(eid="e1", authornv=("a2", "1"), authorsec=gen.g2_1),
            gen.status(eid="e1", authornv=("a3", "1"), authorsec=gen.g3_1),
            gen.status(eid="e1", authornv=("a4", "1"), authorsec=gen.g1_1),
            gen.status(eid="e1", authornv=("a5", "1"), authorsec=gen.g2_1),
            gen.status(eid="e1", authornv=("a6", "1"), authorsec=gen.g3_1),
        ]

        status.create_status(self.writer, statuses)
        self.flush()

        self.assertEqual(6, len(status.get_statuses(self.writer, sha256="e1")))
        self.assertEqual(2, len(status.get_statuses(self.es1, sha256="e1")))
        self.assertEqual(4, len(status.get_statuses(self.es2, sha256="e1")))
        self.assertEqual(6, len(status.get_statuses(self.es3, sha256="e1")))

    def test_queued_or_filtered(self):
        self.dp_simulate_consumers_on_event_mm.side_effect = lambda *vs, **kv: mapi.EventSimulate(
            consumers=[
                mapi.EventSimulateConsumer(name="p1", version="1", filter_out=False, filter_out_trigger=""),
                mapi.EventSimulateConsumer(name="p2", version="1", filter_out=False, filter_out_trigger=""),
                mapi.EventSimulateConsumer(name="p3", version="1", filter_out=False, filter_out_trigger=""),
                mapi.EventSimulateConsumer(name="p4", version="1", filter_out=False, filter_out_trigger=""),
                mapi.EventSimulateConsumer(name="p5", version="1", filter_out=False, filter_out_trigger=""),
                mapi.EventSimulateConsumer(
                    name="p6", version="1", filter_out=True, filter_out_trigger="gjson|@any:test"
                ),
                mapi.EventSimulateConsumer(
                    name="p7", version="1", filter_out=True, filter_out_trigger="gjson|@any:test22"
                ),
            ]
        )
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    authornv=("a1", "1"),
                    fvl=[("f1", "v1"), ("f2", "v1")],
                    timestamp="2000-01-01T05:55:55Z",
                    sourceit=("generic_source", "2000-01-01T04:44:44Z"),
                ),
                # Binary event that has an older timestamp but everything else the same.
                # As this one is older it shouldn't be the timestamp that gest used for pre-filtered and queued events.
                gen.binary_event(
                    eid="e1",
                    authornv=("a1", "1"),
                    fvl=[("f1", "v1"), ("f2", "v1")],
                    timestamp="2000-01-01T00:07:00Z",
                    sourceit=("generic_source", "2000-01-01T00:07:00Z"),
                ),
            ]
        )
        status.create_status(
            self.writer,
            [
                gen.status(eid="e1", authornv=("p1", "1"), status="heartbeat", ts="2000-01-01T00:00:00Z"),
                gen.status(eid="e1", authornv=("p2", "1"), status="error-exception", ts="2000-01-01T00:00:00Z"),
                gen.status(eid="e1", authornv=("p3", "1"), status="heartbeat", ts="2000-01-01T00:00:00Z"),
                gen.status(eid="e1", authornv=("p4", "1"), status="heartbeat", ts="2000-01-01T00:00:00Z"),
            ],
        )
        plugin.create_plugin(
            self.writer,
            [
                gen.plugin(authornv=("p1", "1"), config={"heartbeat_interval": "100"}),
                gen.plugin(authornv=("p2", "1"), config={"heartbeat_interval": "100"}),
                gen.plugin(authornv=("p3", "1"), config={"heartbeat_interval": "100"}),
                gen.plugin(authornv=("p4", "1"), config={"heartbeat_interval": "100"}),
                gen.plugin(authornv=("p5", "1"), config={"heartbeat_interval": "100"}),
                gen.plugin(authornv=("p6", "1"), config={"heartbeat_interval": "100"}),
                gen.plugin(authornv=("p7", "1"), config={"heartbeat_interval": "100"}),
                gen.plugin(authornv=("p8", "1"), config={"heartbeat_interval": "100"}),
            ],
        )
        self.flush()

        results = status.get_binary_status(self.writer, "e1")
        self.assertFormatted(
            results,
            [
                models_restapi.StatusEvent(
                    timestamp="2000-01-01T00:00:00+00:00",
                    author=models_restapi.Author(security="LOW TLP:CLEAR", category="plugin", name="p1", version="1"),
                    entity=models_restapi.StatusEntity(
                        status="heartbeat-lost",
                        error="more than 2 minutes after last heartbeat",
                        runtime=10.0,
                        input=models_restapi.StatusInput(
                            entity=models_restapi.StatusInputEntity(sha256="e1"),
                        ),
                    ),
                    security="LOW TLP:CLEAR",
                ),
                models_restapi.StatusEvent(
                    timestamp="2000-01-01T00:00:00+00:00",
                    author=models_restapi.Author(security="LOW TLP:CLEAR", category="plugin", name="p2", version="1"),
                    entity=models_restapi.StatusEntity(
                        status="error-exception",
                        runtime=10.0,
                        input=models_restapi.StatusInput(
                            entity=models_restapi.StatusInputEntity(sha256="e1"),
                        ),
                    ),
                    security="LOW TLP:CLEAR",
                ),
                models_restapi.StatusEvent(
                    timestamp="2000-01-01T00:00:00+00:00",
                    author=models_restapi.Author(security="LOW TLP:CLEAR", category="plugin", name="p3", version="1"),
                    entity=models_restapi.StatusEntity(
                        status="heartbeat-lost",
                        error="more than 2 minutes after last heartbeat",
                        runtime=10.0,
                        input=models_restapi.StatusInput(
                            entity=models_restapi.StatusInputEntity(sha256="e1"),
                        ),
                    ),
                    security="LOW TLP:CLEAR",
                ),
                models_restapi.StatusEvent(
                    timestamp="2000-01-01T00:00:00+00:00",
                    author=models_restapi.Author(security="LOW TLP:CLEAR", category="plugin", name="p4", version="1"),
                    entity=models_restapi.StatusEntity(
                        status="heartbeat-lost",
                        error="more than 2 minutes after last heartbeat",
                        runtime=10.0,
                        input=models_restapi.StatusInput(
                            entity=models_restapi.StatusInputEntity(sha256="e1"),
                        ),
                    ),
                    security="LOW TLP:CLEAR",
                ),
                models_restapi.StatusEvent(
                    timestamp="2000-01-01T05:55:55+00:00",
                    author=models_restapi.Author(security="LOW TLP:CLEAR", category="plugin", name="p5", version="1"),
                    entity=models_restapi.StatusEntity(
                        status="queued",
                        message="",
                        runtime=0.0,
                        input=models_restapi.StatusInput(
                            entity=models_restapi.StatusInputEntity(sha256="e1"),
                        ),
                    ),
                    security="LOW TLP:CLEAR",
                ),
                models_restapi.StatusEvent(
                    timestamp="2000-01-01T05:55:55+00:00",
                    author=models_restapi.Author(security="LOW TLP:CLEAR", category="plugin", name="p6", version="1"),
                    entity=models_restapi.StatusEntity(
                        status="prefiltered",
                        message="gjson|@any:test",
                        runtime=0.0,
                        input=models_restapi.StatusInput(
                            entity=models_restapi.StatusInputEntity(sha256="e1"),
                        ),
                    ),
                    security="LOW TLP:CLEAR",
                ),
                models_restapi.StatusEvent(
                    timestamp="2000-01-01T05:55:55+00:00",
                    author=models_restapi.Author(security="LOW TLP:CLEAR", category="plugin", name="p7", version="1"),
                    entity=models_restapi.StatusEntity(
                        status="prefiltered",
                        message="gjson|@any:test22",
                        runtime=0.0,
                        input=models_restapi.StatusInput(
                            entity=models_restapi.StatusInputEntity(sha256="e1"),
                        ),
                    ),
                    security="LOW TLP:CLEAR",
                ),
            ],
        )
