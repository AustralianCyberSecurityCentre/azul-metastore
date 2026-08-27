"""Test users with specific api_access are limited in what they can do."""

from pstats import Stats

from tests.support import integration_test
from starlette.status import HTTP_403_FORBIDDEN
import json
import os


class TestApiAccess(integration_test.BaseRestapi):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        os.environ["metastore_sources"] = json.dumps(
            {
                "s1": {},
                "samples": {"references": [{"name": "apple", "required": False, "description": "blah"}]},
            }
        )

    def common_apis_to_attempt(self):
        """Common APIs with no protection or api_access test users don't have access to."""
        # Download API
        request_data = {
            # Random sha256
            "sha256": "1aa24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            "source_id": "samples",
            "references": {"apple": "granny smith"},
            "security": "LOW",
        }
        resp = self.client.post("/v0/binaries/source/download", json=request_data)
        self.assertEqual(resp.status_code, HTTP_403_FORBIDDEN)

        # sources
        resp = self.client.get("/v0/sources/")
        self.assertEqual(resp.status_code, HTTP_403_FORBIDDEN)

        resp = self.client.head("/v0/sources/invalid1")
        self.assertEqual(resp.status_code, HTTP_403_FORBIDDEN)

        # Stats (unprotected endpoint)
        resp = self.client.get("/v0/statistics/")
        self.assertEqual(resp.status_code, 200)

        # upload child
        data: list[tuple[str, tuple[None | Unknown, str]]] = [
            ("parent_sha256", (None, "00000000000000000000000000000000000000000000000000000000000000e1")),
            ("relationship", (None, json.dumps({"colour": "blue"}))),
            ("filename", (None, "test.exe")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "LOW")),
            ("settings", (None, json.dumps({"passwords": "abc;def;ghi"}))),
        ]
        resp = self.client.post("/v0/binaries/child?refresh=true", files=data + [("binary", ("file.exe", b"hello"))])
        self.assertEqual(resp.status_code, HTTP_403_FORBIDDEN)

    def test_api_access_upload_only_user(self):
        """Use a user with access to only the upload API."""
        self.client.headers = {"x-test-user": "high_all_upload_api_only"}
        self.common_apis_to_attempt()

        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "samples")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "granny smith"}))),
            ("security", (None, "LOW")),
            ("settings", (None, json.dumps({"passwords": "abc;def;ghi"}))),
        ]
        resp = self.client.post("/v0/binaries/source?refresh=true", files=data + [("binary", ("file.exe", b"hello"))])
        self.assertEqual(resp.status_code, 200)

        # Features not accessible unlike for feature user
        resp = self.client.get("/v0/features")
        self.assertEqual(HTTP_403_FORBIDDEN, resp.status_code)

    def test_api_access_feature_user(self):
        """Use a user with access to only the feature API."""
        self.client.headers = {"x-test-user": "high_all_feature_apis"}
        self.common_apis_to_attempt()

        resp = self.client.get("/v0/features")
        self.assertEqual(200, resp.status_code)

        resp = self.client.post("/v0/features/values/counts", json=dict(items=["f1", "f2"]))
        self.assertEqual(200, resp.status_code)

        # Wasn't given featureTag permissions so doesn't have them
        resp = self.client.post("/v0/features/tags/1?feature=1&value=1", json=dict(security="low"))
        self.assertEqual(HTTP_403_FORBIDDEN, resp.status_code)
