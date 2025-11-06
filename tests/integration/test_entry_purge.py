from click.testing import CliRunner

from azul_metastore import entry_purge
from tests.support import gen, integration_test


class FakeResponse:
    def __init__(self, json=None):
        self.status_code = 200
        self._json = json

    def json(self):
        return self._json


class TestDelete(integration_test.DynamicTestCase):
    def setUp(self):
        self.runner = CliRunner()
        super().setUp()

    def test_submission(self):
        evs = [
            gen.binary_event(eid="e1", sourceit=("s1", "2000-01-01T01:01:01Z")),
            gen.binary_event(eid="e1", sourceit=("s2", "2000-01-01T01:01:01Z")),
            gen.binary_event(eid="e2", sourceit=("s1", "2000-01-01T01:01:01Z")),
            gen.binary_event(eid="e3", sourceit=("s1", "2001-01-01T01:01:01Z")),
        ]
        self.write_binary_events(evs)
        self.assertEqual(3, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e2"))
        self.assertEqual(2, self.count_binary_events("e3"))

        track = evs[3].track_source_references
        # simulation
        res = self.runner.invoke(
            entry_purge.submission,
            [
                track,
                "--timestamp",
                "2000-01-01T01:01:01Z",
            ],
            catch_exceptions=False,
        )
        print(res.stdout)
        self.flush()

        self.assertEqual(res.exit_code, 0)
        self.assertEqual(0, self.dp_submit_events_mm.call_count)

        self.assertEqual(3, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e2"))
        self.assertEqual(2, self.count_binary_events("e3"))

        # actual purge
        res = self.runner.invoke(
            entry_purge.submission,
            [
                track,
                "--timestamp",
                "2000-01-01T01:01:01Z",
                "--purge",
            ],
            catch_exceptions=False,
        )
        print(res.stdout)
        self.flush()

        self.assertEqual(res.exit_code, 0)
        self.assertEqual(1, self.dp_submit_events_mm.call_count)

        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(0, self.count_binary_events("e2"))
        self.assertEqual(2, self.count_binary_events("e3"))

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

        track = evs[2].track_authors[-1]
        res = self.runner.invoke(entry_purge.author, [track, "--purge"], catch_exceptions=False)
        print(res.stdout)
        self.flush()

        self.assertEqual(res.exit_code, 0)
        self.assertEqual(1, self.dp_submit_events_mm.call_count)

        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(0, self.count_binary_events("e2"))
        self.assertEqual(2, self.count_binary_events("e3"))
        self.assertEqual(2, self.count_binary_events("e4"))

    def test_link(self):

        evs = [
            gen.binary_event(eid="e2", authoru="apple"),
            gen.binary_event(eid="e1", authoru="apple"),
            gen.binary_event(
                eid="e1",
                authoru="apple",
                spath=[gen.path(eid="e100", authoru="apple"), gen.path(eid="e10", authoru="apple")],
            ),
            gen.binary_event(eid="e10", authoru="apple", spath=[gen.path(eid="e100", authoru="apple")]),
            gen.binary_event(eid="e100", authoru="apple"),
        ]
        self.write_binary_events(evs)
        self.assertEqual(3, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e10"))
        self.assertEqual(2, self.count_binary_events("e100"))
        self.assertEqual(2, self.count_binary_events("e2"))

        track = evs[3].track_links[-1]
        res = self.runner.invoke(
            entry_purge.link,
            [track, "--purge"],
            catch_exceptions=False,
        )
        print(res.stdout)
        self.flush()

        self.assertEqual(res.exit_code, 0)
        self.assertEqual(1, self.dp_submit_events_mm.call_count)

        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(0, self.count_binary_events("e10"))
        self.assertEqual(2, self.count_binary_events("e100"))
        self.assertEqual(2, self.count_binary_events("e2"))

    def test_link_casing(self):
        evs = [
            gen.binary_event(eid="e2", authoru="apple"),
            gen.binary_event(eid="e1", authoru="apple"),
            gen.binary_event(
                eid="e1",
                authoru="apple",
                spath=[gen.path(eid="e100", authoru="apple"), gen.path(eid="e10", authoru="apple")],
            ),
            gen.binary_event(eid="e10", authoru="apple", spath=[gen.path(eid="e100", authoru="apple")]),
            gen.binary_event(eid="e100", authoru="apple"),
        ]
        self.write_binary_events(evs)
        self.assertEqual(3, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e10"))
        self.assertEqual(2, self.count_binary_events("e100"))
        self.assertEqual(2, self.count_binary_events("e2"))

        track = evs[3].track_links[-1]
        res = self.runner.invoke(
            entry_purge.link,
            [track, "--purge"],
            catch_exceptions=False,
        )
        print(res.stdout)
        self.flush()

        self.assertEqual(res.exit_code, 0)
        self.assertEqual(1, self.dp_submit_events_mm.call_count)

        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(0, self.count_binary_events("e10"))
        self.assertEqual(2, self.count_binary_events("e100"))
        self.assertEqual(2, self.count_binary_events("e2"))

    def test_delete_author(self):
        evs = [
            gen.binary_event(eid="e1"),
            gen.binary_event(eid="e1", authornv=("delete", "1")),
            gen.binary_event(eid="e2", spath=[gen.path(authornv=("delete", "1"))]),
            gen.binary_event(eid="e3", authornv=("deleter", "1")),
            gen.binary_event(eid="e4", spath=[gen.path(authornv=("deleter", "1"))]),
            gen.binary_event(eid="e5", authornv=("delete", "2")),
            gen.binary_event(eid="e6", spath=[gen.path(authornv=("delete", "2"))]),
            gen.binary_event(eid="e7"),
        ]
        self.write_binary_events(evs)

        self.assertEqual(3, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e2"))
        self.assertEqual(2, self.count_binary_events("e3"))
        self.assertEqual(2, self.count_binary_events("e4"))
        self.assertEqual(2, self.count_binary_events("e5"))
        self.assertEqual(2, self.count_binary_events("e6"))
        self.assertEqual(2, self.count_binary_events("e7"))

        track = evs[1].track_authors[-1]
        res = self.runner.invoke(
            entry_purge.author,
            [track, "--purge"],
            catch_exceptions=False,
        )
        print(res.stdout)
        self.flush()

        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(0, self.count_binary_events("e2"))
        self.assertEqual(2, self.count_binary_events("e3"))
        self.assertEqual(2, self.count_binary_events("e4"))
        self.assertEqual(2, self.count_binary_events("e5"))
        self.assertEqual(2, self.count_binary_events("e6"))
        self.assertEqual(2, self.count_binary_events("e7"))

    def test_delete_child_of(self):
        evs = [
            gen.binary_event(eid="e1", authornv=("apple", "1")),
            gen.binary_event(eid="e10", authornv=("apple", "1"), spathl=[("e1", ("apple", "1"))]),
            gen.binary_event(
                eid="e100", authornv=("apple", "1"), spathl=[("e1", ("apple", "1")), ("e10", ("apple", "1"))]
            ),
            gen.binary_event(eid="e2", authornv=("apple", "1")),
            gen.binary_event(eid="e10", authornv=("apple", "1"), spathl=[("e2", ("apple", "1"))]),
        ]

        self.write_binary_events(evs)

        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(3, self.count_binary_events("e10"))
        self.assertEqual(2, self.count_binary_events("e100"))
        self.assertEqual(2, self.count_binary_events("e2"))

        track = evs[1].track_links[-1]
        res = self.runner.invoke(
            entry_purge.link,
            [track, "--purge"],
            catch_exceptions=False,
        )
        print(res.stdout)
        self.flush()

        # e100 is kept as there is a valid path from e2
        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("e10"))
        self.assertEqual(2, self.count_binary_events("e100"))
        self.assertEqual(2, self.count_binary_events("e2"))
