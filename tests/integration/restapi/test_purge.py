import urllib.parse

from tests.support import gen, integration_test


class TestPlugins(integration_test.BaseRestapi):

    def tearDown(self):
        # Ensure header modifications (like added writer user don't live on past the test they're declared in)
        self.client.headers.clear()
        return super().tearDown()

    def test_purge_submission_system(self):
        """Use system creds to purge a submission."""
        evs = [
            gen.binary_event(eid="e1", authornv=("low", "1"), sourceit=("s1", "2000-01-01T01:01:01Z")),
            gen.binary_event(eid="e1", authoru="high", sourceit=("s2", "2000-01-02T01:01:01Z")),
            gen.binary_event(eid="e2", authoru="high", sourceit=("s1", "2000-01-03T01:01:01Z")),
            gen.binary_event(eid="e1", authoru="high", sourceit=("s1", "2000-01-04T01:01:01Z")),
        ]
        self.write_binary_events(evs)
        self.client.headers = {"x-test-user": "writer"}

        # simulate with timestamp filter
        ts = urllib.parse.quote_plus("2000-01-01T01:01:01Z")
        track = evs[0].track_source_references
        response = self.client.delete(f"/v0/purge/submission/{track}?timestamp={ts}")
        self.assertEqual(200, response.status_code, response.json())
        resp = response.json()
        self.assertFormatted(
            resp,
            {
                "data": {
                    "events": 1,
                },
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR", "sec_filter": ""},
            },
        )
        # simulate with no further filters
        response = self.client.delete(f"/v0/purge/submission/{track}")
        self.assertEqual(422, response.status_code, response.json())

        # test that partial deletion from a source does not delete entity
        response = self.client.delete(f"/v0/purge/submission/{track}?timestamp={ts}&purge=true")
        self.assertEqual(200, response.status_code, response.json())
        resp = response.json()
        self.assertFormatted(
            resp,
            {
                "data": {"events_purged": 1, "binaries_kept": 0, "binaries_purged": 0},
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR", "sec_filter": ""},
            },
        )
        self.flush()

        # retrying to delete and there is nothing there
        response = self.client.delete(f"/v0/purge/submission/{track}?timestamp={ts}&purge=true")
        self.assertEqual(400, response.status_code, response.json())

        response = self.client.delete(f"/v0/purge/submission/{track}?purge=true")
        self.assertEqual(422, response.status_code, response.json())

        # Bad but not invalid timestamp
        response = self.client.delete(f"/v0/purge/submission/{track}?timestamp=2025&purge=true")
        self.assertEqual(400, response.status_code, response.json())
        self.assertFormatted(response.json(), {"detail": "nothing to delete"})

        # Completely invalid timestamp
        response = self.client.delete(f"/v0/purge/submission/{track}?timestamp=not-a-timestamp&purge=true")
        self.assertEqual(400, response.status_code, response.json())
        self.assertFormatted(
            response.json(), {"detail": "The timestamp provided 'not-a-timestamp' has an invalid format."}
        )

    def test_purge_submission_low(self):
        """Use system creds to purge a submission."""
        evs = [
            gen.binary_event(eid="e1", authornv=("low", "1"), sourceit=("s1", "2000-01-01T01:01:01Z")),
            gen.binary_event(eid="e1", authoru="high", sourceit=("s2", "2000-01-02T01:01:01Z")),
            gen.binary_event(eid="e2", authoru="high", sourceit=("s1", "2000-01-03T01:01:01Z")),
            gen.binary_event(eid="e1", authoru="high", sourceit=("s1", "2000-01-04T01:01:01Z")),
        ]
        self.write_binary_events(evs)

        # simulate with timestamp filter
        user = "low"
        ts = urllib.parse.quote_plus("2000-01-01T01:01:01Z")
        track = evs[0].track_source_references
        response = self.client.delete(f"/v0/purge/submission/{track}?timestamp={ts}", headers={"x-test-user": user})
        self.assertEqual(403, response.status_code, response.json())
        resp = response.json()
        self.assertFormatted(resp, {"detail": "user 'low' not superuser"})
        # simulate with no further filters
        response = self.client.delete(f"/v0/purge/submission/{track}", headers={"x-test-user": user})
        self.assertEqual(422, response.status_code, response.json())

        # test that deletion is refused
        response = self.client.delete(f"/v0/purge/submission/{track}?purge=true", headers={"x-test-user": user})
        self.assertEqual(422, response.status_code, response.json())

        # test that partial deletion from a source does not delete entity
        response = self.client.delete(
            f"/v0/purge/submission/{track}?purge=true&timestamp={ts}",
            headers={"x-test-user": user},
        )
        self.assertEqual(403, response.status_code, response.json())
        resp = response.json()
        self.assertFormatted(resp, {"detail": "user 'low' not superuser"})

        response = self.client.delete(
            f"/v0/purge/submission/{track}?purge=true&timestamp={ts}",
            headers={"x-test-user": user},
        )
        self.assertEqual(403, response.status_code, response.json())
        resp = response.json()
        self.assertFormatted(resp, {"detail": "user 'low' not superuser"})

    def test_purge_link_system(self):
        evs = [
            gen.binary_event(eid="e1", authornv=("low", "1")),
            gen.binary_event(eid="e10", authornv=("low", "1"), spathl=[("e1", ("low", "1"))]),
            gen.binary_event(eid="e10", authornv=("high", "1"), spathl=[("e1", ("low", "1"))]),
            gen.binary_event(eid="e100", authornv=("low", "1"), spathl=[("e1", ("low", "1")), ("e10", ("low", "1"))]),
            gen.binary_event(
                eid="e100", authornv=("high", "1"), spathl=[("e1", ("low", "1")), ("e10", ("high", "1"))]
            ),
            gen.binary_event(eid="e2", authornv=("low", "1")),
            gen.binary_event(eid="e10", authornv=("low", "1"), spathl=[("e2", ("low", "1"))]),
        ]
        self.write_binary_events(evs)

        track = evs[1].track_links[-1]

        self.client.headers = {"x-test-user": "writer"}

        response = self.client.delete(f"/v0/purge/link/{track}")
        self.assertEqual(200, response.status_code, response.json())
        resp = response.json()
        self.assertFormatted(
            resp,
            {
                "data": {
                    "events": 1,
                },
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR", "sec_filter": ""},
            },
        )
        self.flush()

        response = self.client.delete(f"/v0/purge/link/{track}?purge=true")
        self.assertEqual(200, response.status_code, response.json())
        resp = response.json()
        self.assertFormatted(
            resp,
            {
                "data": {"events_purged": 1, "binaries_kept": 0, "binaries_purged": 0},
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR", "sec_filter": ""},
            },
        )

        self.flush()

        response = self.client.delete(f"/v0/purge/link/{track}?purge=true")
        self.assertEqual(400, response.status_code, response.json())
        resp = response.json()
        self.assertFormatted(resp, {"detail": "nothing to delete"})

        track2 = evs[2].track_links[-1]

        response = self.client.delete(f"/v0/purge/link/{track2}?purge=true")
        self.assertEqual(200, response.status_code, response.json())
        resp = response.json()
        self.assertFormatted(
            resp,
            {
                "data": {"events_purged": 1, "binaries_kept": 0, "binaries_purged": 0},
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR", "sec_filter": ""},
            },
        )

    def test_purge_link_low(self):
        user = "low"
        evs = [
            gen.binary_event(eid="e1", authornv=("low", "1")),
            gen.binary_event(eid="e10", authornv=("low", "1"), spathl=[("e1", None)]),
            gen.binary_event(eid="e10", authoru="high", spathl=[("e1", None)]),
            gen.binary_event(eid="e100", authornv=("low", "1"), spathl=[("e1", None), ("e10", None)]),
            gen.binary_event(eid="e100", authoru="high", spathl=[("e1", None), ("e10", None)]),
            gen.binary_event(eid="e2", authornv=("low", "1")),
            gen.binary_event(eid="e10", authornv=("low", "1"), spathl=[("e2", None)]),
        ]
        self.write_binary_events(evs)

        track = evs[1].track_links[-1]

        response = self.client.delete(
            f"/v0/purge/link/{track}",
            headers={"x-test-user": user},
        )
        self.assertEqual(403, response.status_code, response.json())
        resp = response.json()
        self.assertFormatted(resp, {"detail": "user 'low' not superuser"})

        response = self.client.delete(
            f"/v0/purge/link/{track}?purge=true",
            headers={"x-test-user": user},
        )
        self.assertEqual(403, response.status_code, response.json())
        resp = response.json()
        self.assertFormatted(resp, {"detail": "user 'low' not superuser"})

        response = self.client.delete(
            f"/v0/purge/link/{track}?purge=true",
            headers={"x-test-user": user},
        )
        self.assertEqual(403, response.status_code, response.json())
        resp = response.json()
        self.assertFormatted(resp, {"detail": "user 'low' not superuser"})

        track2 = evs[2].track_links[-1]
        response = self.client.delete(
            f"/v0/purge/link/{track2}?purge=true",
            headers={"x-test-user": user},
        )
        self.assertEqual(403, response.status_code, response.json())
        resp = response.json()
        self.assertFormatted(resp, {"detail": "user 'low' not superuser"})
