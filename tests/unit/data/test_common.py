import datetime
import io
import json
import re
from unittest import mock

from azul_bedrock import models_api
from azul_bedrock import models_network as azm
from azul_bedrock.exceptions import ApiException
from fastapi import UploadFile

from azul_metastore import context
from azul_metastore.common import data_common, data_strings, utils
from azul_metastore.context import Context
from azul_metastore.query.binary2 import binary_submit
from tests.support import basic_test, gen, unit_test

from . import helpers


@mock.patch.object(Context, "get_user_access", helpers.mock_get_user_access)
class CommonTestCases(unit_test.DataMockingUnitTest):
    maxDiff = None

    def setUp(self):
        v = super().setUp()
        self.priv_ctx = context.get_writer_context()
        return v

    @mock.patch("azul_bedrock.dispatcher.DispatcherAPI.submit_events")
    async def test_high_level_submit_binary(self, se: mock.MagicMock):
        se.side_effect = basic_test.resp_submit_events
        ret = await binary_submit.high_level_submit_binary(
            binary=UploadFile(io.BytesIO(b"contents")),
            sha256=None,
            source="source",
            references={},
            parent_sha256="",
            relationship={},
            filename="file.name",
            timestamp="2022-02-02T00:00:00Z",
            security="low",
            extract=False,
            password=None,
            expedite=True,
            user="me",
            ctx=self.ctx,
            priv_ctx=self.priv_ctx,
        )
        print(ret[0].track_source_references)
        evnt = gen.gen_binary_data_as_binary_data(b"contents", azm.DataLabel.CONTENT, "file.name")
        evnt.track_source_references = "source.d41d8cd98f00b204e9800998ecf8427e"
        self.assertEqual(ret[0], evnt)
        event: azm.BinaryEvent = se.call_args[0][0][0]

        dummy = datetime.datetime(2000, 1, 1, 0, 0, 0, 0)
        event.timestamp = dummy
        decoded = json.loads(event.model_dump_json(exclude_defaults=True))
        decoded["timestamp"] = dummy.isoformat()
        print(decoded)
        self.assertEqual(
            decoded,
            {
                "model_version": 5,
                "kafka_key": "tmp",
                "timestamp": dummy.isoformat(),
                "author": {"category": "user", "name": "me", "security": "low"},
                "entity": {
                    "sha256": "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
                    "sha512": "ac98d72fccae58536b132637d9f2220af6e87667db65f3744b7552fb9dfb1c67e3ececb7291bd287bc4a860dca2f7abf417bc89d7ab873cc028f07a24f9f6772",
                    "sha1": "4a756ca07e9487f482465a99e8286abc86ba4dc7",
                    "md5": "98bf7d8c15784f0a3d63204441e1e2aa",
                    "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                    "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                    "size": 8,
                    "file_format": "image/gif",
                    "file_extension": ".gif",
                    "mime": "mimish",
                    "magic": "magical",
                    "features": [
                        {"name": "file_format", "type": "string", "value": "image/gif"},
                        {"name": "file_extension", "type": "string", "value": ".gif"},
                        {"name": "magic", "type": "string", "value": "magical"},
                        {"name": "mime", "type": "string", "value": "mimish"},
                        {"name": "filename", "type": "filepath", "value": "file.name"},
                        {"name": "submission_file_extension", "type": "string", "value": "name"},
                    ],
                    "datastreams": [
                        {
                            "sha256": "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
                            "sha512": "ac98d72fccae58536b132637d9f2220af6e87667db65f3744b7552fb9dfb1c67e3ececb7291bd287bc4a860dca2f7abf417bc89d7ab873cc028f07a24f9f6772",
                            "sha1": "4a756ca07e9487f482465a99e8286abc86ba4dc7",
                            "md5": "98bf7d8c15784f0a3d63204441e1e2aa",
                            "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                            "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "size": 8,
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
                    "security": "low",
                    "name": "source",
                    "timestamp": "2022-02-02T00:00:00+00:00",
                    "path": [
                        {
                            "sha256": "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
                            "action": "sourced",
                            "timestamp": "2022-02-02T00:00:00+00:00",
                            "author": {"category": "user", "name": "me", "security": "low"},
                            "file_format": "image/gif",
                            "size": 8,
                            "filename": "file.name",
                        }
                    ],
                },
            },
        )

        # Expedited
        se.return_value = {}
        ret = await binary_submit.high_level_submit_binary(
            binary=UploadFile(io.BytesIO(b"contents")),
            sha256=None,
            source="source",
            references={},
            parent_sha256="",
            relationship={},
            filename="file.name",
            timestamp="2022-02-02T00:00:00Z",
            security="low",
            extract=False,
            password=None,
            expedite=True,
            user="me",
            ctx=self.ctx,
            priv_ctx=self.priv_ctx,
        )
        evnt = gen.gen_binary_data_as_binary_data(b"contents", "content", "file.name")
        evnt.track_source_references = "source.d41d8cd98f00b204e9800998ecf8427e"
        print(ret[0].track_source_references)
        self.assertEqual(ret[0], evnt)
        events: list[azm.BinaryEvent] = se.call_args[0][0]
        self.assertEqual(len(events), 2)
        dummy = datetime.datetime(2000, 1, 1, 0, 0, 0, 0)
        for e in events:
            e.timestamp = dummy
        decoded = json.loads(events[0].model_dump_json(exclude_defaults=True))
        decoded_expedite = json.loads(events[1].model_dump_json(exclude_defaults=True))
        decoded["timestamp"] = dummy.isoformat()
        print(decoded)
        self.assertEqual(
            decoded,
            {
                "model_version": 5,
                "kafka_key": "tmp",
                "timestamp": "2000-01-01T00:00:00",
                "author": {"category": "user", "name": "me", "security": "low"},
                "entity": {
                    "sha256": "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
                    "sha512": "ac98d72fccae58536b132637d9f2220af6e87667db65f3744b7552fb9dfb1c67e3ececb7291bd287bc4a860dca2f7abf417bc89d7ab873cc028f07a24f9f6772",
                    "sha1": "4a756ca07e9487f482465a99e8286abc86ba4dc7",
                    "md5": "98bf7d8c15784f0a3d63204441e1e2aa",
                    "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                    "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                    "size": 8,
                    "file_format": "image/gif",
                    "file_extension": ".gif",
                    "mime": "mimish",
                    "magic": "magical",
                    "features": [
                        {"name": "file_format", "type": "string", "value": "image/gif"},
                        {"name": "file_extension", "type": "string", "value": ".gif"},
                        {"name": "magic", "type": "string", "value": "magical"},
                        {"name": "mime", "type": "string", "value": "mimish"},
                        {"name": "filename", "type": "filepath", "value": "file.name"},
                        {"name": "submission_file_extension", "type": "string", "value": "name"},
                    ],
                    "datastreams": [
                        {
                            "sha256": "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
                            "sha512": "ac98d72fccae58536b132637d9f2220af6e87667db65f3744b7552fb9dfb1c67e3ececb7291bd287bc4a860dca2f7abf417bc89d7ab873cc028f07a24f9f6772",
                            "sha1": "4a756ca07e9487f482465a99e8286abc86ba4dc7",
                            "md5": "98bf7d8c15784f0a3d63204441e1e2aa",
                            "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                            "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "size": 8,
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
                    "security": "low",
                    "name": "source",
                    "timestamp": "2022-02-02T00:00:00+00:00",
                    "path": [
                        {
                            "sha256": "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
                            "action": "sourced",
                            "timestamp": "2022-02-02T00:00:00+00:00",
                            "author": {"category": "user", "name": "me", "security": "low"},
                            "file_format": "image/gif",
                            "size": 8,
                            "filename": "file.name",
                        }
                    ],
                },
            },
        )
        self.assertTrue(decoded_expedite["flags"]["expedite"])
        self.assertEqual(
            decoded_expedite["entity"]["sha256"],
            "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
        )
        del decoded_expedite["flags"]
        self.assertEqual(decoded_expedite, decoded)

        # some augstreams
        se.return_value = {}
        ret = await binary_submit.high_level_submit_binary(
            binary=UploadFile(io.BytesIO(b"contents")),
            sha256=None,
            source="source",
            references={},
            parent_sha256="",
            relationship={},
            filename="special_file_name",
            timestamp="2022-02-02T00:00:00Z",
            security="low",
            extract=False,
            password=None,
            expedite=True,
            user="me",
            ctx=self.ctx,
            priv_ctx=self.priv_ctx,
            augstreams=[(azm.DataLabel.TEST, b"some_data"), (azm.DataLabel.TEXT, b"other_data")],
        )
        print(ret[0].track_source_references)
        ev = gen.gen_binary_data_as_binary_data(b"contents", azm.DataLabel.CONTENT, "special_file_name")
        ev.track_source_references = "source.d41d8cd98f00b204e9800998ecf8427e"
        self.assertEqual(ret[0], ev)
        event = se.call_args[0][0][0]
        event.timestamp = dummy

        decoded = json.loads(event.model_dump_json(exclude_defaults=True))
        decoded["timestamp"] = dummy.isoformat()
        print(decoded)
        self.assertEqual(
            decoded,
            {
                "model_version": 5,
                "kafka_key": "tmp",
                "timestamp": "2000-01-01T00:00:00",
                "author": {"category": "user", "name": "me", "security": "low"},
                "entity": {
                    "sha256": "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
                    "sha512": "ac98d72fccae58536b132637d9f2220af6e87667db65f3744b7552fb9dfb1c67e3ececb7291bd287bc4a860dca2f7abf417bc89d7ab873cc028f07a24f9f6772",
                    "sha1": "4a756ca07e9487f482465a99e8286abc86ba4dc7",
                    "md5": "98bf7d8c15784f0a3d63204441e1e2aa",
                    "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                    "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                    "size": 8,
                    "file_format": "image/gif",
                    "file_extension": ".gif",
                    "mime": "mimish",
                    "magic": "magical",
                    "features": [
                        {"name": "file_format", "type": "string", "value": "image/gif"},
                        {"name": "file_extension", "type": "string", "value": ".gif"},
                        {"name": "magic", "type": "string", "value": "magical"},
                        {"name": "mime", "type": "string", "value": "mimish"},
                        {"name": "filename", "type": "filepath", "value": "special_file_name"},
                    ],
                    "datastreams": [
                        {
                            "sha256": "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
                            "sha512": "ac98d72fccae58536b132637d9f2220af6e87667db65f3744b7552fb9dfb1c67e3ececb7291bd287bc4a860dca2f7abf417bc89d7ab873cc028f07a24f9f6772",
                            "sha1": "4a756ca07e9487f482465a99e8286abc86ba4dc7",
                            "md5": "98bf7d8c15784f0a3d63204441e1e2aa",
                            "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                            "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "size": 8,
                            "file_format": "image/gif",
                            "file_extension": ".gif",
                            "mime": "mimish",
                            "magic": "magical",
                            "identify_version": 1,
                            "label": "content",
                        },
                        {
                            "sha256": "b48d1de58c39d2160a4b8a5a9cae90818da1212742ec1f11fba1209bed0a212c",
                            "sha512": "9e9c969fe37de2b2daf75e09e17b05e06fe0a3b5fc57522bc12a77c816499219ffea1e73ba4c7853a04bd1d13718ae12882f9dfd633d0323fe66ddaa91fd6328",
                            "sha1": "2eb484cf4b77e41f20c480f3f83ee94689b78cab",
                            "md5": "0d9247cbce34aba4aca8d5c887a0f0a4",
                            "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                            "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "size": 9,
                            "file_format": "image/gif",
                            "file_extension": ".gif",
                            "mime": "mimish",
                            "magic": "magical",
                            "identify_version": 1,
                            "label": "test",
                        },
                        {
                            "sha256": "811c7837440c0209201a1a7dce19550ea353657cd3f4719e3b2f0d47177aeb8e",
                            "sha512": "37515d695450f5f41ec852a40bd4a1ee2153fd57550773dc58c109d64a2426406b496916e9050fc5366e76fd387bfb33e057b35ee75d888e54b1fb38b2bf29f4",
                            "sha1": "5c2e03fdce4c760db63f300e2ad754150cb525e1",
                            "md5": "d3e49519c459082fcc686d57d461f852",
                            "ssdeep": "3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
                            "tlsh": "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "size": 10,
                            "file_format": "image/gif",
                            "file_extension": ".gif",
                            "mime": "mimish",
                            "magic": "magical",
                            "identify_version": 1,
                            "label": "text",
                        },
                    ],
                },
                "action": "sourced",
                "source": {
                    "name": "source",
                    "path": [
                        {
                            "sha256": "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
                            "action": "sourced",
                            "timestamp": "2022-02-02T00:00:00+00:00",
                            "author": {"category": "user", "name": "me", "security": "low"},
                            "file_format": "image/gif",
                            "size": 8,
                            "filename": "special_file_name",
                        }
                    ],
                    "timestamp": "2022-02-02T00:00:00+00:00",
                    "security": "low",
                },
            },
        )

    @mock.patch(
        "azul_metastore.query.binary2.binary_read.find_stream_references", lambda *args: (False, None, None)
    )  # parent_sha256 doesn't exist
    @mock.patch("azul_bedrock.dispatcher.DispatcherAPI.submit_events")
    async def test_high_level_submit_binary_invalid_submissions(self, se: mock.MagicMock):
        se.side_effect = basic_test.resp_submit_events
        # Submit with no source or parent
        with self.assertRaises(ApiException) as apiException:
            await binary_submit.high_level_submit_binary(
                binary=UploadFile(io.BytesIO(b"contents")),
                sha256="1",
                source="",
                references={},
                parent_sha256="",
                relationship={},
                filename="file.name",
                timestamp="2020-06-02 11:47:03.2Z",
                security="low",
                extract=False,
                password=None,
                expedite=True,
                user="me",
                ctx=self.ctx,
                priv_ctx=self.priv_ctx,
            )
        self.assertEqual(apiException.exception.detail["internal"], "no_parent_and_source_submitted")

        # Submit with source and parent
        with self.assertRaises(ApiException) as apiException:
            await binary_submit.high_level_submit_binary(
                binary=UploadFile(io.BytesIO(b"contents")),
                sha256="1",
                source="source1",
                references={},
                parent_sha256="2",
                relationship={},
                filename="file.name",
                timestamp="2020-06-02 11:47:03.2Z",
                security="low",
                extract=False,
                password=None,
                expedite=True,
                user="me",
                ctx=self.ctx,
                priv_ctx=self.priv_ctx,
            )
        self.assertEqual(apiException.exception.detail["internal"], "parent_and_source_both_submitted")

        # Submit with no binary and no sha256
        with self.assertRaises(ApiException) as apiException:
            await binary_submit.high_level_submit_binary(
                binary="",
                sha256="",
                source="source1",
                references={},
                parent_sha256="",
                relationship={},
                filename="file.name",
                timestamp="2020-06-02 11:47:03.2Z",
                security="low",
                extract=False,
                password=None,
                expedite=True,
                user="me",
                ctx=self.ctx,
                priv_ctx=self.priv_ctx,
            )
        self.assertEqual(apiException.exception.detail["internal"], "upload_no_binary_sha256")

        # upload to parent With invalid parent_sha256
        with self.assertRaises(ApiException) as apiException:
            await binary_submit.high_level_submit_binary(
                binary=UploadFile(io.BytesIO(b"contents")),
                sha256="1",
                source="",
                references={},
                parent_sha256="invalid",
                relationship={},
                filename="file.name",
                timestamp="2020-06-02 11:47:03.2Z",
                security="low",
                extract=False,
                password=None,
                expedite=True,
                user="me",
                ctx=self.ctx,
                priv_ctx=self.priv_ctx,
            )
        self.assertEqual(apiException.exception.detail["internal"], "upload_not_found_parent_sha256")

    @mock.patch(
        "azul_metastore.query.binary2.binary_read.find_stream_references", lambda *args: (False, None, None)
    )  # dispatcher returns with an ok length of zero because no events were published.
    @mock.patch("azul_bedrock.dispatcher.DispatcherAPI.submit_events")
    async def test_high_level_submit_dispatcher_empty_ok_response(self, se: mock.MagicMock):
        def wrap_resp_submit_events(*args, **kwargs) -> models_api.ResponsePostEvent:
            """Return ok with length of zero from dispatcher and ensure an appropriate API exception occurs."""
            model = basic_test.resp_submit_events(*args, **kwargs)
            model.total_ok = 0
            model.ok = []
            return model

        se.side_effect = wrap_resp_submit_events
        # Submit with no source or parent
        with self.assertRaises(ApiException) as apiException:
            await binary_submit.high_level_submit_binary(
                binary=UploadFile(io.BytesIO(b"contents")),
                sha256=None,
                source="source",
                references={},
                parent_sha256="",
                relationship={},
                filename="file.name",
                timestamp="2022-02-02T00:00:00Z",
                security="low",
                extract=False,
                password=None,
                expedite=False,
                user="me",
                ctx=self.ctx,
                priv_ctx=self.priv_ctx,
            )
        self.assertEqual(apiException.exception.detail["ref"], "Dispatcher rejected the submitted events")

    def test_basename(self):
        self.assertEqual("filename", data_common.basename("filename"))
        self.assertEqual("unix_filename", data_common.basename("/etc/config/unix_filename"))
        self.assertEqual("Windows.exe", data_common.basename("C:\\Program Files\\CoolStuff\\Windows.exe"))
        self.assertEqual("mixed.exe", data_common.basename("C:\\Program Files\\Cool/Stuff\\mixed.exe"))
        self.assertEqual("relative", data_common.basename("Cool/Stuff\\relative"))

    def test_to_utc(self):
        self.assertEqual("2021-08-12T15:23:11Z", utils.to_utc("2021-08-12T16:23:11+01:00"))

    @staticmethod
    async def make_string_iterable(value: bytes):
        """Read in fixed chunk size to make testing more realistic."""
        CHUNK_SIZE = 5  # Note changing this chunk size will have an impact on the test results.
        location = 0
        while location + CHUNK_SIZE < len(value):
            yield value[location : location + CHUNK_SIZE]
            location += CHUNK_SIZE
        # yield remainder
        if location < len(value):
            yield value[location:]

    async def test_get_strings(self):
        strings, file_length, more_data = await data_strings.get_strings(
            self.make_string_iterable(b"hello"), 1, 200, 0
        )
        self.assertEqual(file_length, 5)
        self.assertEqual(more_data, False)
        self.assertEqual(1, len(strings))
        self.assertEqual({"string": "hello", "offset": 0, "length": 5, "encoding": "ASCII"}, strings[0].model_dump())

        # big string dropped
        data = b"a" * 999999
        strings, file_length, more_data = await data_strings.get_strings(self.make_string_iterable(data), 1, 200, 0)
        self.assertEqual(file_length, len(data))
        self.assertEqual(more_data, False)
        self.assertEqual(0, len(strings))

        # big string kept
        strings, file_length, more_data = await data_strings.get_strings(
            self.make_string_iterable(data), 1, 9999990, 0
        )
        self.assertEqual(file_length, len(data))
        self.assertEqual(more_data, False)
        self.assertEqual(1, len(strings))
        self.assertEqual(data.decode(), strings[0].string)
        self.assertEqual(
            {"string": data.decode(), "offset": 0, "length": 999999, "encoding": "ASCII"},
            strings[0].model_dump(),
        )

        # character flow, just check we always get the same amount
        data = b"word"
        for i in range(128):
            data += (i).to_bytes(1, byteorder="big") + b"word"

        strings, file_length, more_data = await data_strings.get_strings(self.make_string_iterable(data), 1, 200, 0)
        self.assertEqual(file_length, len(data))
        self.assertEqual(more_data, False)
        self.assertEqual(33, len(strings))

    async def test_get_strings_complex(self):
        """Note these tests are highly affected by chunk size, as it changes total bytes read."""

        # verify a cap out at max strings requested.
        data = b"word"
        for i in range(128):
            data += (i).to_bytes(1, byteorder="big") + b"word"

        # Test the buffer limiting and max strings to find.
        # Also test when there is more data available
        strings, file_length, more_data = await data_strings.get_strings(
            self.make_string_iterable(data), 1, 100, 0, strings_to_read_before_stopping=5, buffer_size=100
        )
        self.assertEqual(file_length, 100)
        self.assertEqual(more_data, True)
        self.assertEqual(20, len(strings))

        data = (
            b"word"
            + (1).to_bytes(1, byteorder="big")
            + (2).to_bytes(1, byteorder="big")
            + b"word"
            + (3).to_bytes(1, byteorder="big")
            + b"findme"
        )

        # Test find all strings for data ("word", "word", "findme")
        strings, file_length, more_data = await data_strings.get_strings(self.make_string_iterable(data), 1, 10, 0)
        self.assertEqual(file_length, len(data))
        self.assertEqual(more_data, False)
        self.assertEqual(3, len(strings))

        # This catches a lot of edge cases around overlap of buffers and finding strings at the end of a buffer.
        strings, file_length, more_data = await data_strings.get_strings(
            self.make_string_iterable(data), 1, 10, 0, buffer_size=10
        )
        self.assertEqual(file_length, len(data))
        self.assertEqual(more_data, False)
        # Account for finding only part of a string near the boundary of a chunk e.g
        # word**word*findme if you make the chunk here 'word**word*find' findme will never be a result.
        found = [s.string for s in strings]
        expected = ["word", "word", "findme"]
        self.assertCountEqual(found, expected)

        # Verify max strings stops the search prematurely and less than the possible strings are found
        strings, file_length, more_data = await data_strings.get_strings(
            self.make_string_iterable(data), 1, 4, 0, strings_to_read_before_stopping=1, buffer_size=4
        )
        self.assertEqual(file_length, 5)  # Read one chunk
        self.assertGreater(len(data), file_length)  # Verify we didn't read all the contents
        self.assertEqual(more_data, True)
        self.assertEqual(1, len(strings))

        # Verify can find string with search param looking for word
        strings, file_length, more_data = await data_strings.get_strings(
            self.make_string_iterable(data), 1, 10, 0, find_string="ord"
        )
        self.assertEqual(file_length, len(data))
        self.assertEqual(2, len(strings))

        # Verify regex work as expected
        strings, file_length, more_data = await data_strings.get_strings(
            # Find find and findme
            self.make_string_iterable(data),
            1,
            10,
            0,
            find_regex=re.compile("f.*"),
        )
        self.assertEqual(file_length, len(data))
        self.assertEqual(1, len(strings))
