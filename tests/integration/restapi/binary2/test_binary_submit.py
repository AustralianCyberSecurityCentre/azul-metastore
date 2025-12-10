import hashlib
import io
import json
import os

import cart
from azul_bedrock import models_network as azm

from tests.support import gen, integration_test


class TestBinaryRead(integration_test.BaseRestapi):
    def assertStatusCode(self, response, responsecode):
        try:
            self.assertEqual(response.status_code, responsecode)
        except Exception:
            print(f"{response.status_code} - {response.content}")
            raise

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        os.environ["metastore_sources"] = json.dumps(
            {
                "s1": {},
                "samples": {"references": [{"name": "apple", "required": False, "description": "blah"}]},
            }
        )

    def bad_security_base(self, endpoint: str, extra_data: list[str]):
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "samples")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "granny smith"}))),
            ("security", (None, "apple banana carrot durian eggplant fig")),
        ]
        response = self.client.post(f"/v0/binaries/{endpoint}", files=data + extra_data)
        # should fail since the security items are unregistered
        self.assertStatusCode(response, 422)

    # test security for all endpoints.

    def test_security_source(self):
        self.bad_security_base(
            "source?refresh=true", [("source_id", (None, "samples")), ("binary", ("file.exe", b"hello"))]
        )

    def test_security_source_dataless(self):
        self.bad_security_base(
            "source/dataless?refresh=true",
            [("source_id", (None, "samples")), ("sha256", (None, hashlib.sha256(b"hello").hexdigest()))],
        )

    def test_security_child(self):
        self.bad_security_base(
            "child",
            [
                ("parent_sha256", (None, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")),
                ("binary", ("file.exe", b"hello")),
            ],
        )

    def test_security_child_dataless(self):
        self.bad_security_base(
            "child/dataless",
            [
                ("parent_sha256", (None, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")),
                ("sha256", (None, hashlib.sha256(b"hello").hexdigest())),
            ],
        )

    def test_no_security(self):
        # security is required and absence should raise an error
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "samples")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "granny smith"}))),
        ]
        response = self.client.post(
            "/v0/binaries/source?refresh=true", files=data + [("binary", ("file.exe", b"hello"))]
        )
        self.assertStatusCode(response, 422)

    def test_simple(self):
        # upload normal
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "samples")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "granny smith"}))),
            ("security", (None, "LOW")),
            ("settings", (None, json.dumps({"passwords": "abc;def;ghi"}))),
        ]
        response = self.client.post(
            "/v0/binaries/source?refresh=true", files=data + [("binary", ("file.exe", b"hello"))]
        )
        self.assertEqual(self.async_dp_submit_binary_mm.call_args_list[0].args[0], "samples")
        self.assertEqual(self.async_dp_submit_binary_mm.call_args_list[0].args[1], "content")
        # Verify file size was the expected length (can't check contents because the file is closed but the hash catches that.)
        self.assertEqual(self.async_dp_submit_binary_mm.call_args_list[0].args[2].size, 5)
        self.assertStatusCode(response, 200)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        saved = response.json()[0]
        self.assertFormatted(
            saved,
            {
                "md5": "5d41402abc4b2a76b9719d911017c592",
                "sha1": "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
                "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                "sha512": "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043",
                "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "size": 5,
                "file_format_legacy": "GIF",
                "file_format": "image/gif",
                "file_extension": ".gif",
                "mime": "mimish",
                "magic": "magical",
                "filename": "test.exe",
                "track_source_references": "samples.7123efebf32ff232278417e61d135857",
                "label": "content",
            },
        )
        # Verify expedite flag is set on second event.
        dp_events = self.get_dp_events()
        self.assertEqual(len(dp_events), 2)
        self.assertTrue(dp_events[1]["flags"]["expedite"])

        # check metastore has the document
        response = self.client.get(f"/v0/binaries/{saved['sha256']}")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertFormatted(
            resp["data"]["streams"][0],
            {
                "md5": "5d41402abc4b2a76b9719d911017c592",
                "sha1": "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
                "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                "sha512": "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043",
                "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "size": 5,
                "file_format_legacy": "GIF",
                "file_format": "image/gif",
                "file_extension": ".gif",
                "mime": "mimish",
                "magic": "magical",
                "identify_version": 1,
                "label": ["content"],
                "instances": [
                    "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.sourced"
                ],
            },
        )

        dp_events = self.get_dp_events()
        self.assertEqual(len(dp_events), 2)
        self.assertFormatted(
            dp_events[0],
            {
                "model_version": 5,
                "kafka_key": "tmp",
                "timestamp": "2024-01-22T01:00:00+00:00",
                "author": {"category": "user", "name": "high_all", "security": "LOW"},
                "entity": {
                    "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                    "sha512": "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043",
                    "sha1": "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
                    "md5": "5d41402abc4b2a76b9719d911017c592",
                    "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                    "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                    "size": 5,
                    "file_format_legacy": "GIF",
                    "file_format": "image/gif",
                    "file_extension": ".gif",
                    "mime": "mimish",
                    "magic": "magical",
                    "features": [
                        {"name": "file_format", "type": "string", "value": "image/gif"},
                        {"name": "file_format_legacy", "type": "string", "value": "GIF"},
                        {"name": "file_extension", "type": "string", "value": ".gif"},
                        {"name": "magic", "type": "string", "value": "magical"},
                        {"name": "mime", "type": "string", "value": "mimish"},
                        {"name": "filename", "type": "filepath", "value": "test.exe"},
                        {"name": "submission_file_extension", "type": "string", "value": "exe"},
                    ],
                    "datastreams": [
                        {
                            "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                            "sha512": "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043",
                            "sha1": "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
                            "md5": "5d41402abc4b2a76b9719d911017c592",
                            "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                            "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "size": 5,
                            "file_format_legacy": "GIF",
                            "file_format": "image/gif",
                            "file_extension": ".gif",
                            "mime": "mimish",
                            "magic": "magical",
                            "identify_version": 1,
                            "label": "content",
                        }
                    ],
                },
                "action": "sourced",
                "source": {
                    "security": "LOW",
                    "name": "samples",
                    "timestamp": "2020-06-02T11:47:03.200000+00:00",
                    "references": {"apple": "granny smith"},
                    "path": [
                        {
                            "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                            "action": "sourced",
                            "timestamp": "2024-01-22T01:00:00+00:00",
                            "author": {"category": "user", "name": "high_all", "security": "LOW"},
                            "file_format_legacy": "GIF",
                            "file_format": "image/gif",
                            "size": 5,
                            "filename": "test.exe",
                        }
                    ],
                    "settings": {"passwords": "abc;def;ghi", "remove_at_depth": "2"},
                },
            },
        )

    def test_invalid_parameters(self):
        # upload normal
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "samples")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "LOW")),
        ]
        # Invalid settings (not valid json) with valid references
        data_copy = data.copy()
        data_copy.append(("settings", (None, "Invalid settings!")))
        data_copy.append(("references", (None, json.dumps({"apple": "granny smith"}))))
        response = self.client.post(
            "/v0/binaries/source?refresh=true", files=data_copy + [("binary", ("file.exe", b"hello"))]
        )
        self.assertEqual(response.status_code, 422)

        # Invalid references (not valid json)
        data_copy = data.copy()
        data_copy.append(("references", (None, "Invalid references!")))
        response = self.client.post(
            "/v0/binaries/source?refresh=true", files=data_copy + [("binary", ("file.exe", b"hello"))]
        )
        self.assertEqual(response.status_code, 422)

    def test_timezone_utc(self):
        # UTC
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "samples")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "granny smith"}))),
            ("security", (None, "LOW")),
        ]
        response = self.client.post(
            "/v0/binaries/source?refresh=true", files=data + [("binary", ("file.exe", b"hello"))]
        )
        self.assertStatusCode(response, 200)
        saved = response.json()[0]
        response = self.client.get(f"/v0/binaries/{saved['sha256']}")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(resp["data"]["sources"][0]["direct"][0]["timestamp"], "2020-06-02T11:47:03.200000Z")

        # no TZ - assume utc
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "samples")),
            ("timestamp", (None, "2020-06-02 11:47:03.2")),
            ("references", (None, json.dumps({"apple": "granny smith"}))),
            ("security", (None, "LOW")),
        ]
        response = self.client.post(
            "/v0/binaries/source?refresh=true", files=data + [("binary", ("file.exe", b"hello2"))]
        )
        self.assertStatusCode(response, 200)
        saved = response.json()[0]
        response = self.client.get(f"/v0/binaries/{saved['sha256']}")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(resp["data"]["sources"][0]["direct"][0]["timestamp"], "2020-06-02T11:47:03.200000Z")

        # other TZ - convert to utc
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "samples")),
            ("timestamp", (None, "2020-06-02 11:47:03.2-07:00")),
            ("references", (None, json.dumps({"apple": "granny smith"}))),
            ("security", (None, "LOW")),
        ]
        response = self.client.post(
            "/v0/binaries/source?refresh=true", files=data + [("binary", ("file.exe", b"hello3"))]
        )
        self.assertStatusCode(response, 200)
        saved = response.json()[0]
        response = self.client.get(f"/v0/binaries/{saved['sha256']}")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        self.assertEqual(resp["data"]["sources"][0]["direct"][0]["timestamp"], "2020-06-02T18:47:03.200000Z")

    def test_child_no_security(self):
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
        # upload normal
        data = [
            ("parent_sha256", (None, "00000000000000000000000000000000000000000000000000000000000000e1")),
            ("relationship", (None, json.dumps({"colour": "blue"}))),
            ("filename", (None, "test.exe")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
        ]
        response = self.client.post(
            "/v0/binaries/child?refresh=true", files=data + [("binary", ("file.exe", b"hello"))]
        )
        self.assertStatusCode(response, 422)

    def test_child_submit(self):
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
        # upload normal
        data = [
            ("parent_sha256", (None, "00000000000000000000000000000000000000000000000000000000000000e1")),
            ("relationship", (None, json.dumps({"colour": "blue"}))),
            ("filename", (None, "test.exe")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "LOW")),
            ("settings", (None, json.dumps({"passwords": "abc;def;ghi"}))),
        ]
        response = self.client.post(
            "/v0/binaries/child?refresh=true", files=data + [("binary", ("file.exe", b"hello"))]
        )
        self.assertStatusCode(response, 200)
        sha256 = response.json()[0]["sha256"]

        print(self.async_dp_submit_binary_mm.call_args_list)
        self.assertEqual(self.async_dp_submit_binary_mm.call_args_list[0].args[0], "s1")
        self.assertEqual(self.async_dp_submit_binary_mm.call_args_list[0].args[1], "content")
        # Verify file size was the expected length (can't check contents because the file is closed but the hash catches that.)
        self.assertEqual(self.async_dp_submit_binary_mm.call_args_list[0].args[2].size, 5)

        # check metastore has the document
        response = self.client.get(f"/v0/binaries/{sha256}")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        resp["data"].pop("documents")
        self.assertFormatted(
            resp["data"],
            {
                "security": ["LOW TLP:CLEAR"],
                "sources": [
                    {
                        "source": "s1",
                        "direct": [],
                        "indirect": [
                            {
                                "security": "LOW TLP:CLEAR",
                                "name": "s1",
                                "timestamp": "2022-02-02T00:00:00Z",
                                "references": {"ref2": "val2", "ref1": "val1"},
                                "settings": {"remove_at_depth": "3", "passwords": "abc;def;ghi"},
                                "track_source_references": "s1.dd6e233ae7a843de99f9b43c349069e4",
                            }
                        ],
                    }
                ],
                "parents": [
                    {
                        "sha256": "00000000000000000000000000000000000000000000000000000000000000e1",
                        "action": "extracted",
                        "timestamp": "2020-06-02T11:47:03.200000Z",
                        "author": {"category": "user", "name": "high_all", "security": "LOW"},
                        "relationship": {"colour": "blue"},
                        "file_format_legacy": "Text",
                        "file_format": "text/plain",
                        "size": 1024,
                        "track_link": "00000000000000000000000000000000000000000000000000000000000000e1.2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.None",
                    }
                ],
                "children": [],
                "instances": [
                    {
                        "key": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted",
                        "author": {"security": "LOW", "category": "user", "name": "high_all"},
                        "action": "extracted",
                        "num_feature_values": 7,
                    }
                ],
                "features": [
                    {
                        "name": "file_extension",
                        "type": "string",
                        "value": ".gif",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "file_format",
                        "type": "string",
                        "value": "image/gif",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "file_format_legacy",
                        "type": "string",
                        "value": "GIF",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "filename",
                        "type": "filepath",
                        "value": "test.exe",
                        "label": [],
                        "parts": {"filepath": "test.exe"},
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "magic",
                        "type": "string",
                        "value": "magical",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "mime",
                        "type": "string",
                        "value": "mimish",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "submission_file_extension",
                        "type": "string",
                        "value": "exe",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    },
                ],
                "streams": [
                    {
                        "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                        "sha512": "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043",
                        "sha1": "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
                        "md5": "5d41402abc4b2a76b9719d911017c592",
                        "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                        "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                        "size": 5,
                        "file_format_legacy": "GIF",
                        "file_format": "image/gif",
                        "file_extension": ".gif",
                        "mime": "mimish",
                        "magic": "magical",
                        "identify_version": 1,
                        "label": ["content"],
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    }
                ],
                "info": [],
                "tags": [],
            },
        )

        dp_events = self.get_dp_events()
        self.assertEqual(len(dp_events), 1)
        self.assertFormatted(
            dp_events[0],
            {
                "model_version": 5,
                "kafka_key": "meta-tmp",
                "timestamp": "2024-01-22T01:00:00+00:00",
                "author": {"category": "user", "name": "high_all", "security": "LOW"},
                "entity": {
                    "original_source": "s1",
                    "parent_sha256": "00000000000000000000000000000000000000000000000000000000000000e1",
                    "child": {
                        "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                        "sha512": "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043",
                        "sha1": "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
                        "md5": "5d41402abc4b2a76b9719d911017c592",
                        "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                        "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                        "size": 5,
                        "file_format_legacy": "GIF",
                        "file_format": "image/gif",
                        "file_extension": ".gif",
                        "mime": "mimish",
                        "magic": "magical",
                        "features": [
                            {"name": "file_format", "type": "string", "value": "image/gif"},
                            {"name": "file_format_legacy", "type": "string", "value": "GIF"},
                            {"name": "file_extension", "type": "string", "value": ".gif"},
                            {"name": "magic", "type": "string", "value": "magical"},
                            {"name": "mime", "type": "string", "value": "mimish"},
                            {"name": "filename", "type": "filepath", "value": "test.exe"},
                            {"name": "submission_file_extension", "type": "string", "value": "exe"},
                        ],
                        "datastreams": [
                            {
                                "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                                "sha512": "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043",
                                "sha1": "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
                                "md5": "5d41402abc4b2a76b9719d911017c592",
                                "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                                "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                                "size": 5,
                                "file_format_legacy": "GIF",
                                "file_format": "image/gif",
                                "file_extension": ".gif",
                                "mime": "mimish",
                                "magic": "magical",
                                "identify_version": 1,
                                "label": "content",
                            }
                        ],
                    },
                    "child_history": {
                        "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                        "action": "extracted",
                        "timestamp": "2020-06-02T11:47:03.200000+00:00",
                        "author": {"category": "user", "name": "high_all", "security": "LOW"},
                        "relationship": {"colour": "blue"},
                        "file_format_legacy": "GIF",
                        "file_format": "image/gif",
                        "size": 5,
                        "filename": "test.exe",
                    },
                },
            },
        )

    def test_child_submit_contentless_parent(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="00000000000000000000000000000000000000000000000000000000000000e1",
                    sourceit=("s1", "2022-02-02T00:00+00:00"),
                    authornv=("a1", "1"),
                    fvl=[("magic", "text/plain"), ("mime", "text/plain")],
                    # timestamp="2022-02-02T00:00+00:00",
                ),
                gen.binary_event(
                    # This won't have content:
                    {"action": azm.BinaryAction.Enriched},
                    eid="00000000000000000000000000000000000000000000000000000000000000e1",
                    sourceit=("s1", "2023-02-02T00:00+00:00"),
                    authornv=("a2", "2"),
                    fvl=[("magic", "text/plain"), ("mime", "text/plain")],
                    # timestamp="2023-02-02T00:00+00:00",
                ),
            ]
        )
        # upload normal
        data = [
            ("parent_sha256", (None, "00000000000000000000000000000000000000000000000000000000000000e1")),
            ("relationship", (None, json.dumps({"colour": "blue"}))),
            ("filename", (None, "test.exe")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "LOW")),
        ]
        response = self.client.post(
            "/v0/binaries/child?refresh=true", files=data + [("binary", ("file.exe", b"hello"))]
        )
        self.assertStatusCode(response, 200)
        sha256 = response.json()[0]["sha256"]

        print(self.async_dp_submit_binary_mm.call_args_list)
        self.assertEqual(self.async_dp_submit_binary_mm.call_args_list[0].args[0], "s1")
        self.assertEqual(self.async_dp_submit_binary_mm.call_args_list[0].args[1], "content")
        # Verify file size was the expected length (can't check contents because the file is closed but the hash catches that.)
        self.assertEqual(self.async_dp_submit_binary_mm.call_args_list[0].args[2].size, 5)

        # check metastore has the document with only the non-enriched parent
        response = self.client.get(f"/v0/binaries/{sha256}")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        resp["data"].pop("documents")
        self.assertFormatted(
            resp["data"],
            {
                "security": ["LOW TLP:CLEAR"],
                "sources": [
                    {
                        "source": "s1",
                        "direct": [],
                        "indirect": [
                            {
                                "security": "LOW TLP:CLEAR",
                                "name": "s1",
                                "timestamp": "2022-02-02T00:00:00Z",
                                "references": {"ref2": "val2", "ref1": "val1"},
                                "track_source_references": "s1.dd6e233ae7a843de99f9b43c349069e4",
                            }
                        ],
                    }
                ],
                "parents": [
                    {
                        "sha256": "00000000000000000000000000000000000000000000000000000000000000e1",
                        "action": "extracted",
                        "timestamp": "2020-06-02T11:47:03.200000Z",
                        "author": {"category": "user", "name": "high_all", "security": "LOW"},
                        "relationship": {"colour": "blue"},
                        "file_format_legacy": "Text",
                        "file_format": "text/plain",
                        "size": 1024,
                        "track_link": "00000000000000000000000000000000000000000000000000000000000000e1.2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.None",
                    }
                ],
                "children": [],
                "instances": [
                    {
                        "key": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted",
                        "author": {"security": "LOW", "category": "user", "name": "high_all"},
                        "action": "extracted",
                        "num_feature_values": 7,
                    }
                ],
                "features": [
                    {
                        "name": "file_extension",
                        "type": "string",
                        "value": ".gif",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "file_format",
                        "type": "string",
                        "value": "image/gif",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "file_format_legacy",
                        "type": "string",
                        "value": "GIF",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "filename",
                        "type": "filepath",
                        "value": "test.exe",
                        "label": [],
                        "parts": {"filepath": "test.exe"},
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "magic",
                        "type": "string",
                        "value": "magical",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "mime",
                        "type": "string",
                        "value": "mimish",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "submission_file_extension",
                        "type": "string",
                        "value": "exe",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    },
                ],
                "streams": [
                    {
                        "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                        "sha512": "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043",
                        "sha1": "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
                        "md5": "5d41402abc4b2a76b9719d911017c592",
                        "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                        "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                        "size": 5,
                        "file_format_legacy": "GIF",
                        "file_format": "image/gif",
                        "file_extension": ".gif",
                        "mime": "mimish",
                        "magic": "magical",
                        "identify_version": 1,
                        "label": ["content"],
                        "instances": [
                            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.user.high_all.extracted"
                        ],
                    }
                ],
                "info": [],
                "tags": [],
            },
        )

        dp_events = self.get_dp_events()
        self.assertEqual(len(dp_events), 1)
        self.assertFormatted(
            dp_events[0],
            {
                "model_version": 5,
                "kafka_key": "meta-tmp",
                "timestamp": "2024-01-22T01:00:00+00:00",
                "author": {"category": "user", "name": "high_all", "security": "LOW"},
                "entity": {
                    "original_source": "s1",
                    "parent_sha256": "00000000000000000000000000000000000000000000000000000000000000e1",
                    "child": {
                        "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                        "sha512": "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043",
                        "sha1": "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
                        "md5": "5d41402abc4b2a76b9719d911017c592",
                        "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                        "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                        "size": 5,
                        "file_format_legacy": "GIF",
                        "file_format": "image/gif",
                        "file_extension": ".gif",
                        "mime": "mimish",
                        "magic": "magical",
                        "features": [
                            {"name": "file_format", "type": "string", "value": "image/gif"},
                            {"name": "file_format_legacy", "type": "string", "value": "GIF"},
                            {"name": "file_extension", "type": "string", "value": ".gif"},
                            {"name": "magic", "type": "string", "value": "magical"},
                            {"name": "mime", "type": "string", "value": "mimish"},
                            {"name": "filename", "type": "filepath", "value": "test.exe"},
                            {"name": "submission_file_extension", "type": "string", "value": "exe"},
                        ],
                        "datastreams": [
                            {
                                "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                                "sha512": "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043",
                                "sha1": "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
                                "md5": "5d41402abc4b2a76b9719d911017c592",
                                "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                                "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                                "size": 5,
                                "file_format_legacy": "GIF",
                                "file_format": "image/gif",
                                "file_extension": ".gif",
                                "mime": "mimish",
                                "magic": "magical",
                                "identify_version": 1,
                                "label": "content",
                            }
                        ],
                    },
                    "child_history": {
                        "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                        "action": "extracted",
                        "timestamp": "2020-06-02T11:47:03.200000+00:00",
                        "author": {"category": "user", "name": "high_all", "security": "LOW"},
                        "relationship": {"colour": "blue"},
                        "file_format_legacy": "GIF",
                        "file_format": "image/gif",
                        "size": 5,
                        "filename": "test.exe",
                    },
                },
            },
        )

    def test_child_dataless_submit(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="00000000000000000000000000000000000000000000000000000000000000e1",
                    sourceit=("s1", "2022-02-02T00:00+00:00"),
                    authornv=("a1", "1"),
                    fvl=[("magic", "text/plain"), ("mime", "text/plain")],
                ),
                gen.binary_event(
                    eid="00000000000000000000000000000000000000000000000000000000000000e2",
                    sourceit=("s1", "2022-02-02T00:00+00:00"),
                    authornv=("a1", "1"),
                    fvl=[("magic", "text/plain"), ("mime", "text/plain")],
                ),
            ]
        )
        # upload normal
        data = [
            ("parent_sha256", (None, "00000000000000000000000000000000000000000000000000000000000000e1")),
            ("relationship", (None, json.dumps({"colour": "blue"}))),
            ("filename", (None, "test.exe")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "LOW")),
            ("settings", (None, json.dumps({"passwords": "abc;def;ghi"}))),
        ]
        response = self.client.post(
            "/v0/binaries/child/dataless?refresh=true",
            files=data + [("sha256", (None, "00000000000000000000000000000000000000000000000000000000000000e2"))],
        )
        self.assertEqual(self.dp_submit_binary_mm.call_args_list, [])
        self.assertStatusCode(response, 200)
        sha256 = response.json()[0]["sha256"]

        # check metastore has the document
        response = self.client.get(f"/v0/binaries/{sha256}")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        resp["data"].pop("documents")
        self.assertFormatted(
            resp["data"],
            {
                "security": ["LOW TLP:CLEAR"],
                "sources": [
                    {
                        "source": "s1",
                        "direct": [
                            {
                                "security": "LOW TLP:CLEAR",
                                "name": "s1",
                                "timestamp": "2022-02-02T00:00:00Z",
                                "references": {"ref2": "val2", "ref1": "val1"},
                                "track_source_references": "s1.dd6e233ae7a843de99f9b43c349069e4",
                            }
                        ],
                        "indirect": [],
                    }
                ],
                "parents": [
                    {
                        "sha256": "00000000000000000000000000000000000000000000000000000000000000e1",
                        "action": "extracted",
                        "timestamp": "2020-06-02T11:47:03.200000Z",
                        "author": {"category": "user", "name": "high_all", "security": "LOW"},
                        "relationship": {"colour": "blue"},
                        "file_format_legacy": "Text",
                        "file_format": "text/plain",
                        "size": 1024,
                        "track_link": "00000000000000000000000000000000000000000000000000000000000000e1.00000000000000000000000000000000000000000000000000000000000000e2.user.high_all.None",
                    }
                ],
                "children": [],
                "instances": [
                    {
                        "key": "00000000000000000000000000000000000000000000000000000000000000e2.plugin.a1.sourced",
                        "author": {"security": "LOW TLP:CLEAR", "category": "plugin", "name": "a1", "version": "1"},
                        "action": "sourced",
                        "num_feature_values": 2,
                    },
                    {
                        "key": "00000000000000000000000000000000000000000000000000000000000000e2.user.high_all.extracted",
                        "author": {"security": "LOW", "category": "user", "name": "high_all"},
                        "action": "extracted",
                        "num_feature_values": 7,
                    },
                ],
                "features": [
                    {
                        "name": "file_extension",
                        "type": "string",
                        "value": "txt",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e2.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "file_format",
                        "type": "string",
                        "value": "text/plain",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e2.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "file_format_legacy",
                        "type": "string",
                        "value": "Text",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e2.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "filename",
                        "type": "filepath",
                        "value": "test.exe",
                        "label": [],
                        "parts": {"filepath": "test.exe"},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e2.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "magic",
                        "type": "string",
                        "value": "ASCII text",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e2.user.high_all.extracted"
                        ],
                    },
                    {
                        "name": "magic",
                        "type": "string",
                        "value": "text/plain",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e2.plugin.a1.sourced"
                        ],
                    },
                    {
                        "name": "mime",
                        "type": "string",
                        "value": "text/plain",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e2.plugin.a1.sourced",
                            "00000000000000000000000000000000000000000000000000000000000000e2.user.high_all.extracted",
                        ],
                    },
                    {
                        "name": "submission_file_extension",
                        "type": "string",
                        "value": "exe",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e2.user.high_all.extracted"
                        ],
                    },
                ],
                "streams": [
                    {
                        "sha256": "00000000000000000000000000000000000000000000000000000000000000e2",
                        "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2",
                        "sha1": "0000000000000000000000000000000000000000",
                        "md5": "00000000000000000000000000000000",
                        "ssdeep": "1:1:1",
                        "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                        "size": 1024,
                        "file_format_legacy": "Text",
                        "file_format": "text/plain",
                        "file_extension": "txt",
                        "mime": "text/plain",
                        "magic": "ASCII text",
                        "identify_version": 1,
                        "label": ["content"],
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e2.plugin.a1.sourced",
                            "00000000000000000000000000000000000000000000000000000000000000e2.user.high_all.extracted",
                        ],
                    }
                ],
                "info": [],
                "tags": [],
            },
        )
        dp_events = self.get_dp_events()
        self.assertEqual(len(dp_events), 1)
        self.assertFormatted(
            dp_events[0],
            {
                "model_version": 5,
                "kafka_key": "meta-tmp",
                "timestamp": "2024-01-22T01:00:00+00:00",
                "author": {"category": "user", "name": "high_all", "security": "LOW"},
                "entity": {
                    "original_source": "s1",
                    "parent_sha256": "00000000000000000000000000000000000000000000000000000000000000e1",
                    "child": {
                        "sha256": "00000000000000000000000000000000000000000000000000000000000000e2",
                        "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2",
                        "sha1": "0000000000000000000000000000000000000000",
                        "md5": "00000000000000000000000000000000",
                        "ssdeep": "1:1:1",
                        "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                        "size": 1024,
                        "file_format_legacy": "Text",
                        "file_format": "text/plain",
                        "file_extension": "txt",
                        "mime": "text/plain",
                        "magic": "ASCII text",
                        "features": [
                            {"name": "file_format", "type": "string", "value": "text/plain"},
                            {"name": "file_format_legacy", "type": "string", "value": "Text"},
                            {"name": "file_extension", "type": "string", "value": "txt"},
                            {"name": "magic", "type": "string", "value": "ASCII text"},
                            {"name": "mime", "type": "string", "value": "text/plain"},
                            {"name": "filename", "type": "filepath", "value": "test.exe"},
                            {"name": "submission_file_extension", "type": "string", "value": "exe"},
                        ],
                        "datastreams": [
                            {
                                "sha256": "00000000000000000000000000000000000000000000000000000000000000e2",
                                "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2",
                                "sha1": "0000000000000000000000000000000000000000",
                                "md5": "00000000000000000000000000000000",
                                "ssdeep": "1:1:1",
                                "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                                "size": 1024,
                                "file_format_legacy": "Text",
                                "file_format": "text/plain",
                                "file_extension": "txt",
                                "mime": "text/plain",
                                "magic": "ASCII text",
                                "identify_version": 1,
                                "label": "content",
                            }
                        ],
                    },
                    "child_history": {
                        "sha256": "00000000000000000000000000000000000000000000000000000000000000e2",
                        "action": "extracted",
                        "timestamp": "2020-06-02T11:47:03.200000+00:00",
                        "author": {"category": "user", "name": "high_all", "security": "LOW"},
                        "relationship": {"colour": "blue"},
                        "file_format_legacy": "Text",
                        "file_format": "text/plain",
                        "size": 1024,
                        "filename": "test.exe",
                    },
                },
            },
        )

    def test_child_submit_cart(self):
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

        istream = io.BytesIO(b"hello")
        ostream = io.BytesIO()
        cart.pack_stream(istream, ostream)
        fdata = ostream.getvalue()

        # upload normal
        data = [
            ("parent_sha256", (None, "00000000000000000000000000000000000000000000000000000000000000e1")),
            ("relationship", (None, json.dumps({"colour": "blue"}))),
            ("filename", (None, "test.exe")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "LOW")),
        ]
        response = self.client.post("/v0/binaries/child?refresh=true", files=data + [("binary", ("file.exe", fdata))])
        self.assertStatusCode(response, 200)

    def test_dataless(self):
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
        # upload normal
        data = [
            ("source_id", (None, "samples")),
            ("references", (None, json.dumps({"apple": "granny smith"}))),
            ("filename", (None, "test.exe")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "LOW")),
            ("sha256", (None, "00000000000000000000000000000000000000000000000000000000000000e1")),
        ]
        response = self.client.post("/v0/binaries/source/dataless?refresh=true", files=data)
        self.assertStatusCode(response, 200)

        self.assertEqual(self.dp_submit_binary_mm.call_args_list, [])
        # check metastore has the document
        response = self.client.get(f"/v0/binaries/00000000000000000000000000000000000000000000000000000000000000e1")
        self.assertEqual(200, response.status_code)
        resp = response.json()
        resp["data"].pop("documents")
        self.assertFormatted(
            resp["data"],
            {
                "security": ["LOW", "LOW TLP:CLEAR"],
                "sources": [
                    {
                        "source": "s1",
                        "direct": [
                            {
                                "security": "LOW TLP:CLEAR",
                                "name": "s1",
                                "timestamp": "2022-02-02T00:00:00Z",
                                "references": {"ref2": "val2", "ref1": "val1"},
                                "track_source_references": "s1.dd6e233ae7a843de99f9b43c349069e4",
                            }
                        ],
                        "indirect": [],
                    },
                    {
                        "source": "samples",
                        "direct": [
                            {
                                "security": "LOW",
                                "name": "samples",
                                "timestamp": "2020-06-02T11:47:03.200000Z",
                                "references": {"apple": "granny smith"},
                                "track_source_references": "samples.7123efebf32ff232278417e61d135857",
                            }
                        ],
                        "indirect": [],
                    },
                ],
                "parents": [],
                "children": [],
                "instances": [
                    {
                        "key": "00000000000000000000000000000000000000000000000000000000000000e1.plugin.a1.sourced",
                        "author": {"security": "LOW TLP:CLEAR", "category": "plugin", "name": "a1", "version": "1"},
                        "action": "sourced",
                        "num_feature_values": 2,
                    },
                    {
                        "key": "00000000000000000000000000000000000000000000000000000000000000e1.user.high_all.sourced",
                        "author": {"security": "LOW", "category": "user", "name": "high_all"},
                        "action": "sourced",
                        "num_feature_values": 7,
                    },
                ],
                "features": [
                    {
                        "name": "file_extension",
                        "type": "string",
                        "value": "txt",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e1.user.high_all.sourced"
                        ],
                    },
                    {
                        "name": "file_format",
                        "type": "string",
                        "value": "text/plain",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e1.user.high_all.sourced"
                        ],
                    },
                    {
                        "name": "file_format_legacy",
                        "type": "string",
                        "value": "Text",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e1.user.high_all.sourced"
                        ],
                    },
                    {
                        "name": "filename",
                        "type": "filepath",
                        "value": "test.exe",
                        "label": [],
                        "parts": {"filepath": "test.exe"},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e1.user.high_all.sourced"
                        ],
                    },
                    {
                        "name": "magic",
                        "type": "string",
                        "value": "ASCII text",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e1.user.high_all.sourced"
                        ],
                    },
                    {
                        "name": "magic",
                        "type": "string",
                        "value": "text/plain",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e1.plugin.a1.sourced"
                        ],
                    },
                    {
                        "name": "mime",
                        "type": "string",
                        "value": "text/plain",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e1.plugin.a1.sourced",
                            "00000000000000000000000000000000000000000000000000000000000000e1.user.high_all.sourced",
                        ],
                    },
                    {
                        "name": "submission_file_extension",
                        "type": "string",
                        "value": "exe",
                        "label": [],
                        "parts": {},
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e1.user.high_all.sourced"
                        ],
                    },
                ],
                "streams": [
                    {
                        "sha256": "00000000000000000000000000000000000000000000000000000000000000e1",
                        "sha512": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e1",
                        "sha1": "0000000000000000000000000000000000000000",
                        "md5": "00000000000000000000000000000000",
                        "ssdeep": "1:1:1",
                        "tlsh": "T10000000000000000000000000000000000000000000000000000000000000000000000",
                        "size": 1024,
                        "file_format_legacy": "Text",
                        "file_format": "text/plain",
                        "file_extension": "txt",
                        "mime": "text/plain",
                        "magic": "ASCII text",
                        "identify_version": 1,
                        "label": ["content"],
                        "instances": [
                            "00000000000000000000000000000000000000000000000000000000000000e1.plugin.a1.sourced",
                            "00000000000000000000000000000000000000000000000000000000000000e1.user.high_all.sourced",
                        ],
                    }
                ],
                "info": [],
                "tags": [],
            },
        )
