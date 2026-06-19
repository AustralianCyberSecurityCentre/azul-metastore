import json
from unittest import mock

from pendulum import UTC, DateTime, Timezone, datetime
import pendulum

from azul_metastore.query import annotation
from tests.support import gen, integration_test


import cart
from azul_bedrock import models_network as azm

from tests.support import gen, integration_test
from azul_metastore.settings import get as get_metastore_settings

TEST_SHA256 = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"


class TestBinaryDownload(integration_test.BaseRestapi):
    @mock.patch("pendulum.now", lambda tz=None: pendulum.parse("2023-10-10T10:10:10Z"))
    def test_simple_download(self):
        """Download event should be generated in dispatcher upon request."""
        request_data = {
            "sha256": TEST_SHA256,
            "source_id": "samples",
            "references": {"apple": "granny smith"},
            "security": "LOW",
        }

        response = self.client.post("/v0/binaries/source/download", json=request_data)
        self.assertEqual(response.status_code, 200)

        dp_call_args = self.dp_submit_events_mm.call_args_list
        download_request_kwargs = dp_call_args[0].kwargs
        self.assertEqual(download_request_kwargs.get("model"), azm.ModelType.Download.value)
        self.assertTrue(download_request_kwargs.get("include_ok"))
        self.assertFormatted(
            download_request_kwargs.get("events"),
            [
                azm.DownloadEvent(
                    model_version=azm.CURRENT_MODEL_VERSION,
                    kafka_key="meta-temp",
                    timestamp=DateTime(2023, 10, 10, 10, 10, 10, tzinfo=Timezone("UTC")),
                    author=azm.Author(category="user", name="high_all"),
                    entity=azm.DownloadEvent.Entity(
                        hash="2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
                    ),
                    source=azm.Source(
                        security="LOW",
                        name="samples",
                        timestamp=DateTime(2023, 10, 10, 10, 10, 10, tzinfo=Timezone("UTC")),
                        references={"apple": "granny smith"},
                        path=[],
                    ),
                    action=azm.DownloadAction.Requested,
                )
            ],
        )

    @mock.patch("pendulum.now", lambda tz=None: pendulum.parse("2023-10-10T10:10:10Z"))
    def test_get_download_plugin_status(self):
        self.write_plugin_events([gen.plugin(authornv=("a1", "1"), config={"is_processing_download_events": "true"})])
        self.write_plugin_events([gen.plugin(authornv=("a2", "1"))])

        response = self.client.get(
            f"/v0/binaries/source/download/{TEST_SHA256}",
        )
        self.assertEqual(response.status_code, 404)

        # Create a download request (immediately indexed due to expedite set to true)
        request_data = {
            "sha256": TEST_SHA256,
            "source_id": "samples",
            "references": {"apple": "granny smith"},
            "security": "LOW",
        }

        response = self.client.post("/v0/binaries/source/download", json=request_data)

        response = self.client.get(
            f"/v0/binaries/source/download/{TEST_SHA256}",
        )
        self.assertEqual(response.status_code, 200)
        print(response.json())
        self.assertEqual(
            response.json(),
            {
                "data": [
                    {
                        "timestamp": "",
                        "author": {"security": "", "category": "", "name": "a1", "version": "1"},
                        "entity": {
                            "status": "queued",
                            "runtime": 0.0,
                            "input": {
                                "entity": {
                                    "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
                                }
                            },
                        },
                        "completed": 0,
                        "security": "",
                    }
                ],
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLEO", "sec_filter": None},
            },
        )

        # Create status events to represent progress of picked up jobs.
        self.write_binary_events([gen.binary_event(authornv=("a1", "1"))])
        now = pendulum.now(tz=UTC)
        now = now.add(days=1)
        self.write_status_events(
            [
                gen.status(eid=f"{TEST_SHA256}", authornv=("a1", "1"), ts=now, status=azm.StatusEnum.COMPLETED.value),
                # Creat an event from a non-download plugin that should be ignored
                gen.status(eid=f"{TEST_SHA256}", authornv=("a2", "1"), ts=now, status=azm.StatusEnum.COMPLETED.value),
            ]
        )

        response = self.client.get(
            f"/v0/binaries/source/download/{TEST_SHA256}",
        )
        self.assertEqual(response.status_code, 200)
        print(response.json())
        self.assertEqual(
            response.json(),
            {
                "data": [
                    {
                        "timestamp": "2023-10-11T10:10:10+00:00",
                        "author": {"security": "LOW TLP:CLEAR", "category": "plugin", "name": "a1", "version": "1"},
                        "entity": {
                            "status": "completed",
                            "runtime": 10.0,
                            "input": {
                                "entity": {
                                    "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
                                }
                            },
                        },
                        "completed": 1,
                        "security": "LOW TLP:CLEAR",
                    },
                ],
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLEO", "sec_filter": None},
            },
        )

        # Include the download request in the response
        response = self.client.get(
            f"/v0/binaries/source/download/{TEST_SHA256}?include_download_requests=true",
        )
        self.assertEqual(response.status_code, 200)
        print(response.json())
        self.assertEqual(
            response.json(),
            {
                "data": [
                    {
                        "timestamp": "2023-10-11T10:10:10+00:00",
                        "author": {"security": "LOW TLP:CLEAR", "category": "plugin", "name": "a1", "version": "1"},
                        "entity": {
                            "status": "completed",
                            "runtime": 10.0,
                            "input": {
                                "entity": {
                                    "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
                                }
                            },
                        },
                        "completed": 1,
                        "security": "LOW TLP:CLEAR",
                    },
                    {
                        "timestamp": "2023-10-10T10:10:10+00:00",
                        "author": {"security": "LOW", "category": "user", "name": "high_all", "version": None},
                        "entity": {
                            "status": "download-requested",
                            "error": "",
                            "message": "Download was requested and is pending.",
                            "runtime": 0.0,
                            "input": {
                                "entity": {
                                    "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
                                }
                            },
                        },
                        "completed": 0,
                        "security": "LOW",
                    },
                ],
                "meta": {"security": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLEO", "sec_filter": None},
            },
        )

    async def test_simple_download(self):
        """Download event should fail when readonly mode is set."""
        get_metastore_settings().readonly_mode = True
        request_data = {
            "sha256": TEST_SHA256,
            "source_id": "samples",
            "references": {"apple": "granny smith"},
            "security": "LOW",
        }
        response = self.client.post("/v0/binaries/source/download", json=request_data)
        self.assertEqual(423, response.status_code)
        j = json.loads(response.text)
        print(j["detail"]["internal"])
        self.assertEqual(j["detail"]["internal"], ExceptionCodeEnum.MetastoreReadOnlyMode.value)
