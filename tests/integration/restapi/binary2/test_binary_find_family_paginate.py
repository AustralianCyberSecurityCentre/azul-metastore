import hashlib
import json

from tests.support import gen, integration_test


class TestBinaryFind(integration_test.BaseRestapi):
    def test_binary_find_all_parents(self):
        for i in range(51):
            eid = f"{i:064x}"

            self.write_binary_events(
                [
                    gen.binary_event(
                        eid=eid,
                        sourceit=("s1", "2022-02-02T00:00+00:00"),
                        authornv=("a1", "1"),
                        fvl=[("magic", "text/plain"), ("mime", "text/plain")],
                    ),
                ]
            )

            data = [
                ("parent_sha256", (None, eid)),
                ("relationship", (None, json.dumps({"colour": "blue"}))),
                ("filename", (None, "test.exe")),
                ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
                ("security", (None, "LOW")),
                ("binary", ("Child.exe", "this is the child".encode("utf-8"))),
            ]

            response = self.client.post("/v0/binaries/child?refresh=true", files=data)
            self.assertEqual(200, response.status_code)

        content = "this is the child".encode("utf-8")
        sha256_hash = hashlib.sha256(content).hexdigest()
        response = self.client.post("/v0/binaries/all/parents/?family_sha256=" + sha256_hash)
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        self.assertEqual(50, len(parsed["data"]["items"]))
        self.assertEqual(parsed["data"]["total"], 51)
        after = parsed["data"]["after"]
        self.assertEqual(after, '{"sha256": "0000000000000000000000000000000000000000000000000000000000000031"}')

    def test_binary_find_all_children(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="00000000000000000000000000000000000000000000000000000000000000e1",
                    sourceit=("s1", "2022-02-02T00:00+00:00"),
                    authornv=("a1", "1"),
                    fvl=[("magic", "text/plain"), ("mime", "text/plain")],
                ),
            ]
        )
        response = self.client.get("/v0/binaries/00000000000000000000000000000000000000000000000000000000000000e1")
        self.assertEqual(200, response.status_code)
        for i in range(50):
            filename = f"file_{i:02}.exe"
            content = f"hello from file {i}".encode("utf-8")

            data = [
                ("parent_sha256", (None, "00000000000000000000000000000000000000000000000000000000000000e1")),
                ("relationship", (None, json.dumps({"colour": "blue"}))),
                ("filename", (None, "test.exe")),
                ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
                ("security", (None, "LOW")),
                ("binary", (filename, content)),
            ]

            response = self.client.post("/v0/binaries/child?refresh=true", files=data)
            self.assertEqual(200, response.status_code)

        filename = f"file_last.exe"
        content = f"this is the last file".encode("utf-8")

        data = [
            ("parent_sha256", (None, "00000000000000000000000000000000000000000000000000000000000000e1")),
            ("relationship", (None, json.dumps({"colour": "blue"}))),
            ("filename", (None, "test.exe")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "LOW")),
            ("binary", (filename, content)),
        ]

        response = self.client.post("/v0/binaries/child?refresh=true", files=data)
        self.assertEqual(200, response.status_code)

        response = self.client.post(
            "/v0/binaries/all/children/?family_sha256=00000000000000000000000000000000000000000000000000000000000000e1"
        )
        self.assertEqual(200, response.status_code)
        parsed = response.json()
        self.assertEqual(50, len(parsed["data"]["items"]))
        self.assertEqual(parsed["data"]["total"], 51)
        after = parsed["data"]["after"]
        self.assertEqual(after, '{"sha256": "e5cb6f6c161f5bf2cf90303197c641b94ec9b4f997174364b6ffb0fffe19f9bf"}')
