import time

from azul_metastore.query import cache
from tests.support import gen, integration_test


class TestStatistics(integration_test.BaseRestapi):
    def test_valid_result(self):
        self.write_binary_events([gen.binary_event(sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"ref1": "a"})])

        self.flush()

        response = self.client.get("/v0/statistics/")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(1, resp["data"]["binary_count"])

    def test_caching(self):
        # Fresh cache should ignore actual count of binaries
        self.write_binary_events([gen.binary_event(sourceit=("s1", "2000-01-01T00:00:00Z"), sourcerefs={"ref1": "a"})])
        cache.store_generic(
            self.writer, "statistics", "global", "v1", {"timestamp": int(time.time()), "data": {"binary_count": 999}}
        )

        self.flush()

        response = self.client.get("/v0/statistics/")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(999, resp["data"]["binary_count"])
