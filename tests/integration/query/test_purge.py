from azul_bedrock import models_network as azm
from azul_bedrock.models_restapi.purge import PurgeResults, PurgeSimulation

from azul_metastore.query import purge as qpurge
from tests.support import gen, integration_test


class FakeResponse:
    def __init__(self, json=None):
        self.status_code = 200
        self._json = json

    def json(self):
        return self._json


class TestPurge(integration_test.DynamicTestCase):

    def setUp(self):
        super().setUp()
        self.purger = qpurge.Purger()

    def test_perform_meta_deletion(self):
        # nonsense entity
        ev = self.purger._delete_event(
            self.writer,
            azm.DeleteAction.author,
            azm.DeleteEvent.DeleteEntity(
                reason="deleted via cli",
                author=azm.DeleteEvent.DeleteEntity.DeleteAuthor(
                    track_author="e1",
                    # Timestamp that is newer than the binary events (higher overall date)
                    # This ensure the events will be deleted.
                    timestamp="2025-01-01T01:01:01Z",
                ),
            ),
        )

        self.write_binary_events(
            [
                gen.binary_event(eid="e1", sourceit=("s1", "2000-01-01T01:01:01Z")),
                gen.binary_event(eid="e1", sourceit=("s2", "2000-01-01T01:01:01Z")),
                gen.binary_event(eid="e2", sourceit=("s1", "2000-01-01T01:01:01Z")),
            ]
        )
        self.assertEqual(3, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e2"))
        # bad query, no matches
        deleted, _ = self.purger._perform_meta_deletion(self.writer, {"query": {"term": {"sha257": "e1"}}}, ev)
        self.flush()
        self.assertEqual(0, deleted)
        self.assertEqual(3, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e2"))

        deleted, _ = self.purger._perform_meta_deletion(self.writer, {"query": {"term": {"sha256": "e1"}}}, ev)
        self.flush()
        self.assertEqual(3, deleted)
        self.assertEqual(0, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e2"))

    def test_purge_submission_simulate(self):
        evs = [
            gen.binary_event(eid="e1", authornv=("low", "1"), sourceit=("s1", "2000-01-01T01:01:01Z")),
            gen.binary_event(eid="e1", authornv=("high", "1"), sourceit=("s2", "2000-01-02T01:01:01Z")),
            gen.binary_event(eid="e2", authornv=("high", "1"), sourceit=("s1", "2000-01-03T01:01:01Z")),
        ]
        self.write_binary_events(evs)
        self.assertEqual(3, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e2"))

        track_source_references = evs[2].track_source_references

        # simulate specific timestamp
        ret = self.purger.purge_submission(
            track_source_references=track_source_references,
            timestamp="2000-01-01T01:01:01Z",
            purge=False,
        )
        self.assertFormatted(
            ret,
            PurgeSimulation(
                events=1,
            ),
        )

        # simulate
        ret = self.purger.purge_submission(
            track_source_references=track_source_references,
            timestamp=None,
            purge=False,
        )
        self.assertFormatted(
            ret,
            PurgeSimulation(
                events=2,
            ),
        )

    def test_purge_submission(self):
        evs = [
            gen.binary_event(eid="e1", sourceit=("s1", "2000-01-01T01:01:01Z")),
            gen.binary_event(eid="e1", sourceit=("s2", "2000-01-01T01:01:01Z")),
            gen.binary_event(eid="e2", sourceit=("s1", "2000-01-01T01:01:01Z")),
            gen.binary_event(eid="e10", sourceit=("s1", "2000-01-01T01:01:01Z"), spathl=[("e1", ("apple", "1"))]),
            gen.binary_event(eid="e100", sourceit=("s1", "2000-01-01T01:01:01Z"), spathl=[("e10", ("apple", "1"))]),
        ]
        self.write_binary_events(evs)
        self.assertEqual(3, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e2"))
        self.assertEqual(2, self.count_binary_events("e10"))
        self.assertEqual(2, self.count_binary_events("e100"))

        track_source_references = evs[2].track_source_references

        # actual purge
        ret = self.purger.purge_submission(
            track_source_references=track_source_references,
            timestamp="2000-01-01T01:01:01Z",
            purge=True,
        )
        self.assertFormatted(ret, PurgeResults(events_purged=7, binaries_kept=0, binaries_purged=4))

        self.flush()
        self.assertEqual(1, self.dp_submit_events_mm.call_count)
        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(0, self.count_binary_events("e2"))
        self.assertEqual(0, self.count_binary_events("e10"))
        self.assertEqual(0, self.count_binary_events("e100"))

    def test_purge_submission_no_refresh(self):
        evs = [
            gen.binary_event(eid="e1", sourceit=("s1", "2000-01-01T01:01:01Z")),
            gen.binary_event(eid="e1", sourceit=("s2", "2000-01-01T01:01:01Z")),
            gen.binary_event(eid="e2", sourceit=("s1", "2000-01-01T01:01:01Z")),
        ]
        self.write_binary_events(evs, refresh=False)

        track_source_references = evs[2].track_source_references
        # actual purge
        ret = self.purger.purge_submission(
            track_source_references=track_source_references,
            timestamp="2000-01-01T01:01:01Z",
            purge=True,
        )
        self.assertFormatted(ret, PurgeResults(events_purged=3, binaries_kept=0, binaries_purged=2))

        self.flush()
        self.assertEqual(1, self.dp_submit_events_mm.call_count)
        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(0, self.count_binary_events("e2"))

    def test_plugin(self):
        evs = [
            gen.binary_event(eid="e1"),
            gen.binary_event(eid="e1", spath=[gen.path(authornv=("deleteme", "123"))]),
            gen.binary_event(eid="e2", authornv=("deleteme", "123")),
            gen.binary_event(eid="e3", authornv=("deleteme", "321")),
            gen.binary_event(eid="e4", spath=[gen.path(authornv=("deleteme", "321"))]),
        ]
        self.write_binary_events(evs)
        self.assertEqual(3, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e2"))
        self.assertEqual(2, self.count_binary_events("e3"))
        self.assertEqual(2, self.count_binary_events("e4"))

        # delete author deleteme:123
        # find correct tracking field to delete with
        track_author = evs[2].track_authors[-1]
        print(f"{track_author=}")

        ret = self.purger.purge_author(track_author=track_author, purge=True)
        self.assertFormatted(ret, PurgeResults(events_purged=3, binaries_purged=1, binaries_kept=0))
        self.flush()

        self.assertEqual(1, self.dp_submit_events_mm.call_count)

        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(0, self.count_binary_events("e2"))
        self.assertEqual(2, self.count_binary_events("e3"))
        self.assertEqual(2, self.count_binary_events("e4"))

    def test_link(self):
        evs = [
            gen.binary_event(eid="e2", authornv=("apple", "1")),
            gen.binary_event(eid="e1", authornv=("apple", "1")),
            gen.binary_event(
                eid="e1",
                authornv=("apple", "1"),
                spathl=[("e100", ("apple", "1")), ("e10", ("apple", "1"))],
            ),
            gen.binary_event(eid="e10", authornv=("apple", "1"), spathl=[("e100", ("apple", "1"))]),
            gen.binary_event(eid="e100", authornv=("apple", "1")),
        ]
        self.write_binary_events(evs)
        self.assertEqual(3, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e10"))
        self.assertEqual(2, self.count_binary_events("e100"))
        self.assertEqual(2, self.count_binary_events("e2"))

        # delete link between e100 and e10
        # find correct tracking field to delete with
        track_link = evs[3].track_links[0]
        print(f"{track_link=}")

        ret = self.purger.purge_link(track_link=track_link, purge=True)
        self.assertFormatted(ret, PurgeResults(events_purged=3, binaries_kept=0, binaries_purged=1))
        self.flush()
        self.assertEqual(1, self.dp_submit_events_mm.call_count)
        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(0, self.count_binary_events("e10"))
        self.assertEqual(2, self.count_binary_events("e100"))
        self.assertEqual(2, self.count_binary_events("e2"))

    def test_delete_author(self):
        evs = [
            gen.binary_event(eid="e1", authornv=("p1", "1")),
            gen.binary_event(eid="e2", authornv=("p2", "1"), spathl=[("e1", ("p1", "1"))]),
            gen.binary_event(eid="e3", authornv=("p3", "1"), spathl=[("e1", ("p1", "1")), ("e2", ("p2", "1"))]),
            gen.binary_event(
                eid="e4", authornv=("p4", "1"), spathl=[("e1", ("p1", "1")), ("e2", ("p2", "1")), ("e3", ("p3", "1"))]
            ),
            gen.binary_event(
                eid="e5",
                authornv=("p5", "1"),
                spathl=[("e1", ("p1", "1")), ("e2", ("p2", "1")), ("e3", ("p3", "1")), ("e4", ("p4", "1"))],
            ),
            gen.binary_event(
                eid="e6",
                authornv=("p5", "1"),
                spathl=[("e1", ("p1", "1")), ("e2", ("p2", "1")), ("e3", ("p3", "1")), ("e4", ("p4", "1"))],
            ),
            # ff
            gen.binary_event(eid="f1", authornv=("ff", "1"), spathl=[("e1", ("p1", "1"))]),
            gen.binary_event(eid="e4", authornv=("p4", "1"), spathl=[("e1", ("p1", "1")), ("f1", ("ff", "1"))]),
            # e5 also has unrelated parent
            gen.binary_event(eid="e5", authornv=("unrelated", "1"), spathl=[("e99", ("unrelated", "1"))]),
        ]
        self.write_binary_events(evs)

        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e2"))
        self.assertEqual(2, self.count_binary_events("e3"))
        self.assertEqual(3, self.count_binary_events("e4"))
        self.assertEqual(3, self.count_binary_events("e5"))
        self.assertEqual(2, self.count_binary_events("e6"))
        self.assertEqual(2, self.count_binary_events("f1"))

        # delete plugin ff, which will delete binary f1
        # this should delete binary f1 totally (2 events) and 1 event for e4
        track_author = evs[6].track_authors[-1]  # authornv=("ff", "1")
        print(f"{track_author=}")
        print(evs[2].track_authors)

        ret = self.purger.purge_author(track_author=track_author, purge=True)
        print("did purge")
        self.assertFormatted(ret, PurgeResults(events_purged=3, binaries_kept=0, binaries_purged=1))
        self.flush()
        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e2"))
        self.assertEqual(2, self.count_binary_events("e3"))
        self.assertEqual(2, self.count_binary_events("e4"))
        self.assertEqual(3, self.count_binary_events("e5"))
        self.assertEqual(2, self.count_binary_events("e6"))
        self.assertEqual(0, self.count_binary_events("f1"))

        # delete plugin p2,
        # this should delete e2,e3,e4,e6 and 1 event for e5
        track_author = evs[1].track_authors[-1]  # authornv=("p2", "1")
        ret = self.purger.purge_author(track_author=track_author, purge=True)
        print("did purge")
        self.assertFormatted(ret, PurgeResults(events_purged=9, binaries_kept=0, binaries_purged=4))
        self.flush()
        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(0, self.count_binary_events("e2"))
        self.assertEqual(0, self.count_binary_events("e3"))
        self.assertEqual(0, self.count_binary_events("e4"))
        self.assertEqual(2, self.count_binary_events("e5"))
        self.assertEqual(0, self.count_binary_events("e6"))
        self.assertEqual(0, self.count_binary_events("f1"))

    def test_delete_child_of(self):
        evs = [
            gen.binary_event(eid="e1", authornv=("apple", "1")),
            gen.binary_event(eid="e10", authornv=("apple", "1"), spathl=[("e1", ("apple", "1"))]),
            gen.binary_event(
                eid="e100", authornv=("apple", "1"), spathl=[("e1", ("apple", "1")), ("e10", ("apple", "1"))]
            ),
            gen.binary_event(
                eid="e200", authornv=("apple", "1"), spathl=[("e1", ("apple", "1")), ("e10", ("apple", "1"))]
            ),
            gen.binary_event(eid="e2", authornv=("apple", "1")),
            gen.binary_event(eid="e100", authornv=("apple", "1"), spathl=[("e2", ("apple", "1"))]),
        ]
        self.write_binary_events(evs)

        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e10"))
        self.assertEqual(3, self.count_binary_events("e100"))
        self.assertEqual(2, self.count_binary_events("e200"))
        self.assertEqual(2, self.count_binary_events("e2"))

        # delete relationship between e1 and e10
        # find correct tracking field to delete with
        track_link = evs[1].track_links[-1]
        print(f"{track_link=}")
        print(evs[2].track_links)

        ret = self.purger.purge_link(track_link=track_link, purge=True)
        self.assertFormatted(ret, PurgeResults(events_purged=5, binaries_kept=0, binaries_purged=2))
        self.flush()
        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(0, self.count_binary_events("e10"))
        self.assertEqual(2, self.count_binary_events("e100"))
        self.assertEqual(0, self.count_binary_events("e200"))
        self.assertEqual(2, self.count_binary_events("e2"))
