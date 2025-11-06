import json

from tests.support import gen, integration_test


class TestBinaryFind(integration_test.BaseRestapi):
    def test_binary_find_simple(self):
        self.write_binary_events([gen.binary_event(eid=f"e{x}", authornv=("a1", "1")) for x in range(210)])

        response = self.client.post("/v0/binaries/all?num_binaries=100")
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        self.assertEqual(100, len(parsed["data"]["items"]))
        after = parsed["data"]["after"]
        self.assertEqual(after, '{"SHA256": "e188"}')
        self.assertEqual(parsed["data"]["total"], 210)

        response = self.client.post("/v0/binaries/all?num_binaries=100", content=json.dumps({"after": after}))
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        self.assertEqual(100, len(parsed["data"]["items"]))
        after = parsed["data"]["after"]
        self.assertEqual(after, '{"SHA256": "e9"}')
        self.assertNotIn("total", parsed["data"])

        response = self.client.post("/v0/binaries/all?num_binaries=100", content=json.dumps({"after": after}))
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        self.assertEqual(10, len(parsed["data"]["items"]))
        after = parsed["data"]["after"]
        self.assertEqual(after, '{"SHA256": "e99"}')
        self.assertNotIn("total", parsed["data"])

        response = self.client.post("/v0/binaries/all?num_binaries=100", content=json.dumps({"after": after}))
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        self.assertEqual(0, len(parsed["data"]["items"]))
        self.assertNotIn("after", parsed["data"])
        self.assertNotIn("total", parsed["data"])

        # fresh request
        response = self.client.post("/v0/binaries/all?num_binaries=100")
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        self.assertEqual(100, len(parsed["data"]["items"]))
        self.assertEqual(parsed["data"]["total"], 210)

    def test_binary_find_free_text(self):
        self.write_binary_events(
            [
                gen.binary_event(eid=f"e1{x}", authornv=("a1", "1"), magicmime=("", "application/zip"))
                for x in range(80)
            ]
        )
        self.write_binary_events(
            [
                gen.binary_event(eid=f"e2{x}", authornv=("a1", "1"), magicmime=("", "application/rar"))
                for x in range(60)
            ]
        )

        response = self.client.post("/v0/binaries/all?num_binaries=100", params={"term": '"application/zip"'})
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        self.assertEqual(80, len(parsed["data"]["items"]))
        after = parsed["data"]["after"]
        self.assertEqual(parsed["data"]["total"], 80)

        response = self.client.post(
            "/v0/binaries/all?num_binaries=100",
            params={"term": '"application/zip"'},
            content=json.dumps({"after": after}),
        )
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        self.assertEqual(0, len(parsed["data"]["items"]))
        self.assertNotIn("after", parsed["data"])
        self.assertNotIn("total", parsed["data"])
