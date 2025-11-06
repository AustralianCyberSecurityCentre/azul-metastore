from azul_metastore.query.binary2 import binary_read
from tests.support import gen, integration_test


class TestBinaryRead(integration_test.DynamicTestCase):

    def test_get_author_stats(self):
        ts1 = "2000-01-01T01:00:00.000Z"
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a1", "1"), timestamp=ts1, fvl=[("f1", "v1"), ("f2", "v1")]),
                gen.binary_event(
                    eid="e1", authornv=("a1", "1"), sourceit=("s1", "20100101"), fvl=[("f1", "v1"), ("f2", "v1")]
                ),
                gen.binary_event(eid="e2", authornv=("a1", "1"), timestamp=ts1, fvl=[("f1", "v1"), ("f2", "v1")]),
                gen.binary_event(eid="e3", authornv=("a1", "1"), timestamp=ts1, fvl=[("f2", "v1"), ("f3", "v1")]),
                gen.binary_event(eid="e10", authornv=("a1", "2"), timestamp=ts1, fvl=[("f1", "v1"), ("f2", "v1")]),
                gen.binary_event(eid="e100", authornv=("a2", "1"), timestamp=ts1, fvl=[("f1", "v1"), ("f2", "v1")]),
            ]
        )

        stats = binary_read.get_author_stats(self.writer, "a1", "1")
        self.assertEqual(3, stats)

    def test_entity_new(self):
        ts1 = "2000-01-01T01:00:00.000Z"
        ts2 = "2000-01-01T12:00:00.000Z"
        ts3 = "2000-01-01T20:00:00.000Z"
        ts4 = "2000-01-01T21:00:00.000Z"
        self.write_binary_events([gen.binary_event(eid="e1", authornv=("a1", "1"), timestamp=ts1)])
        self.assertEqual(None, binary_read.get_binary_newer(self.writer, "e1", ts1).newest)
        self.assertEqual(None, binary_read.get_binary_newer(self.writer, "e1", ts2).newest)
        self.assertEqual(None, binary_read.get_binary_newer(self.writer, "e1", ts3).newest)
        self.assertEqual(0, binary_read.get_binary_newer(self.writer, "e1", ts1).count)
        self.assertEqual(0, binary_read.get_binary_newer(self.writer, "e1", ts2).count)
        self.assertEqual(0, binary_read.get_binary_newer(self.writer, "e1", ts3).count)

        self.write_binary_events([gen.binary_event(eid="e1", authornv=("a2", "1"), timestamp=ts2)])
        self.assertEqual(ts2, binary_read.get_binary_newer(self.writer, "e1", ts1).newest)
        self.assertEqual(None, binary_read.get_binary_newer(self.writer, "e1", ts2).newest)
        self.assertEqual(None, binary_read.get_binary_newer(self.writer, "e1", ts3).newest)
        self.assertEqual(1, binary_read.get_binary_newer(self.writer, "e1", ts1).count)
        self.assertEqual(0, binary_read.get_binary_newer(self.writer, "e1", ts2).count)
        self.assertEqual(0, binary_read.get_binary_newer(self.writer, "e1", ts3).count)

        self.write_binary_events([gen.binary_event(eid="e1", authornv=("a3", "1"), timestamp=ts3)])
        self.assertEqual(ts3, binary_read.get_binary_newer(self.writer, "e1", ts1).newest)
        self.assertEqual(ts3, binary_read.get_binary_newer(self.writer, "e1", ts2).newest)
        self.assertEqual(None, binary_read.get_binary_newer(self.writer, "e1", ts3).newest)
        self.assertEqual(2, binary_read.get_binary_newer(self.writer, "e1", ts1).count)
        self.assertEqual(1, binary_read.get_binary_newer(self.writer, "e1", ts2).count)
        self.assertEqual(0, binary_read.get_binary_newer(self.writer, "e1", ts3).count)

        self.write_binary_events([gen.binary_event(eid="e1", authornv=("a4", "1"), timestamp=ts4)])
        self.assertEqual(ts4, binary_read.get_binary_newer(self.writer, "e1", ts1).newest)
        self.assertEqual(ts4, binary_read.get_binary_newer(self.writer, "e1", ts2).newest)
        self.assertEqual(ts4, binary_read.get_binary_newer(self.writer, "e1", ts3).newest)
        self.assertEqual(3, binary_read.get_binary_newer(self.writer, "e1", ts1).count)
        self.assertEqual(2, binary_read.get_binary_newer(self.writer, "e1", ts2).count)
        self.assertEqual(1, binary_read.get_binary_newer(self.writer, "e1", ts3).count)

    def test_list_all_sources_for_binary(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a1", "1"), sourceit=("s1", "2000-01-01T00:00:00Z")),
                gen.binary_event(eid="e1", authornv=("a2", "1"), sourceit=("s2", "2000-01-01T00:00:00Z")),
                gen.binary_event(eid="e1", authornv=("a3", "1"), sourceit=("s3", "2000-01-01T00:00:00Z")),
                gen.binary_event(eid="e1", authornv=("a4", "1"), sourceit=("s4", "2000-01-01T00:00:00Z")),
                gen.binary_event(eid="e1", authornv=("a5", "1"), sourceit=("s5", "2000-01-01T00:00:00Z")),
            ]
        )
        resp = binary_read.list_all_sources_for_binary(self.writer, "e1")
        items = [x for x in resp]
        self.assertEqual(5, len(items))
        self.assertEqual(resp, ["s1", "s2", "s3", "s4", "s5"])

    def test_stream(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a1", "1")),
                gen.binary_event(eid="e1", authornv=("a2", "1")),
                gen.binary_event(eid="e1", authornv=("a3", "1")),
                gen.binary_event(eid="e1", authornv=("a4", "1")),
                gen.binary_event(eid="e1", authornv=("a5", "1")),
            ]
        )
        self.assertEqual(6, self.count_binary_events("e1"))
        self.assertEqual(6, self.count_binary_events("E1"))

    def test_big_data(self):
        bev = gen.binary_event(eid="e1", authornv=("a1", "1"))
        # larger than signed int64
        bev.entity.size = 9_223_372_036_854_775_807 + 100_000
        bev.entity.datastreams[0].size = 9_223_372_036_854_775_807 + 100_000
        bev.source.path[-1].size = 9_223_372_036_854_775_807 + 100_000
        self.write_binary_events([bev])
        self.assertEqual(2, self.count_binary_events("e1"))
        self.assertEqual(2, self.count_binary_events("E1"))

    def test_simple(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1", authornv=("a1", "1"), authorsec=gen.g1_1, sourceit=("s1", "2000-01-01T00:00:00Z")
                ),
            ]
        )

        event = self.read_binary_events("e1")[0]
        self.assertEqual("e1", event["entity"]["sha256"])
        self.assertEqual("plugin", event["author"]["category"])
        self.assertEqual("a1", event["author"]["name"])
        self.assertEqual("1", event["author"]["version"])
        self.assertEqual("2000-01-01T00:00:00Z", event["source"]["timestamp"])
        self.assertEqual(1, len(event["entity"]["datastreams"]))

        event = self.read_binary_events("E1")[0]
        self.assertEqual("e1", event["entity"]["sha256"])
        self.assertEqual("plugin", event["author"]["category"])
        self.assertEqual("a1", event["author"]["name"])
        self.assertEqual("1", event["author"]["version"])
        self.assertEqual("2000-01-01T00:00:00Z", event["source"]["timestamp"])
        self.assertEqual(1, len(event["entity"]["datastreams"]))

    def test_multiple_results(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1", authornv=("a1", "1"), authorsec=gen.g1_1, sourceit=("s1", "2000-01-01T00:00:00Z")
                ),
                gen.binary_event(
                    eid="e1", authornv=("a2", "1"), authorsec=gen.g1_1, sourceit=("s1", "2000-01-01T00:00:00Z")
                ),
            ]
        )

        # read author results for entity 1 & 2
        results = self.read_binary_events("e1")
        self.assertEqual(2, len(results))
        self.assertSetEqual({"a1", "a2"}, {x["author"]["name"] for x in results})

        results = self.read_binary_events("E1")
        self.assertEqual(2, len(results))
        self.assertSetEqual({"a1", "a2"}, {x["author"]["name"] for x in results})

    def test_find_stream_references(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1", authornv=("a1", "1"), authorsec=gen.g1_1, sourceit=("s1", "2000-01-01T00:00:00Z")
                ),
            ]
        )
        result = binary_read.find_stream_references(self.writer, "e1")
        self.assertEqual((True, "s1", "content"), result)
        result = binary_read.find_stream_references(self.writer, "e10")
        self.assertEqual((False, None, None), result)

        result = binary_read.find_stream_references(self.writer, "E1")
        self.assertEqual((True, "s1", "content"), result)
        result = binary_read.find_stream_references(self.writer, "E10")
        self.assertEqual((False, None, None), result)

    def test_binary_count(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", spathl=[]),
            ]
        )

        self.assertEqual(binary_read.get_total_binary_count(self.writer), 1)

        self.write_binary_events(
            [
                gen.binary_event(eid="e2", spathl=[]),
                gen.binary_event(eid="e3", spathl=[]),
            ]
        )
        self.assertEqual(binary_read.get_total_binary_count(self.writer), 3)
