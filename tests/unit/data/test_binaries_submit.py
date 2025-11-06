import copy
import hashlib
import io
import json
from unittest import mock

import cart
import malpz
import pyzipper
from azul_bedrock import models_api
from azul_bedrock import models_network as azm

from azul_metastore.context import Context
from azul_metastore.query.binary2 import binary_submit
from tests.support import gen, unit_test

from . import helpers


# ----------------------------------------------------------------------------------------- Submit Source
@mock.patch("azul_bedrock.dispatcher.DispatcherAPI", unit_test.FakeDispatcherAPI)
@mock.patch.object(Context, "get_user_access", helpers.mock_get_user_access)
class TestSubmitToSource(unit_test.DataMockingUnitTest):
    # FUTURE should not be overwriting standard mock, needs to be accessible
    @mock.patch("azul_bedrock.dispatcher.DispatcherAPI.submit_events")
    async def test_normal(self, se: mock.MagicMock):
        # upload normal
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "user")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "banana"}))),
            ("security", (None, "low TLP:CLEAR TLP:GREEN TLP:AMBER")),
        ]
        se.return_value = models_api.ResponsePostEvent(
            total_ok=1, total_failures=0, failures=[], ok=[gen.binary_event(model=False)]
        )

        response = self.client.post("/v0/binaries/source", files=data + [("binary", ("file.exe", b"hello"))])
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        self.assertEqual(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            j[0]["sha256"],
        )
        # check correct filename
        event: azm.BinaryEvent = se.call_args[0][0][0]
        features = event.entity.features
        filename = [x.value for x in features if x.name == "filename"][0]
        self.assertEqual("test.exe", filename)
        self.assertEqual(response.headers.get("x-azul-security"), "LOW TLP:AMBER")

    @mock.patch("azul_bedrock.dispatcher.DispatcherAPI.submit_events")
    async def test_no_filename(self, se: mock.MagicMock):
        # upload normal but no filename supplied (form filename is ignored)
        data = [
            ("source_id", (None, "user")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "banana"}))),
            ("security", (None, "low TLP:GREEN TLP:AMBER")),
        ]
        se.return_value = models_api.ResponsePostEvent(
            total_ok=1, total_failures=0, failures=[], ok=[gen.binary_event(model=False)]
        )

        response = self.client.post("/v0/binaries/source", files=data + [("binary", ("x", b"hello"))])
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        self.assertEqual(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            j[0]["sha256"],
        )
        # check correct filename
        event: azm.BinaryEvent = se.call_args[0][0][0]
        features = event.entity.features
        filenames = [x.value for x in features if x.name == "filename"]
        self.assertEqual([], filenames)
        self.assertEqual(response.headers.get("x-azul-security"), "LOW TLP:AMBER")

    @mock.patch("azul_bedrock.dispatcher.DispatcherAPI.submit_events")
    async def test_streams(self, se: mock.MagicMock):
        # upload with streams
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "user")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "banana"}))),
            ("security", (None, "low TLP:CLEAR TLP:GREEN TLP:AMBER")),
            ("stream_data", ("proto1.exe", b"data1")),
            ("stream_data", ("proto2.exe", b"data2")),
            ("stream_data", ("proto3.exe", b"data3")),
            ("stream_labels", (None, azm.DataLabel.TEST)),
            ("stream_labels", (None, azm.DataLabel.TEXT)),
            ("stream_labels", (None, azm.DataLabel.DEOB_JS)),
        ]
        se.return_value = models_api.ResponsePostEvent(
            total_ok=1, total_failures=0, failures=[], ok=[gen.binary_event(model=False)]
        )

        response = self.client.post("/v0/binaries/source", files=data + [("binary", ("file.exe", b"hello"))])
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        self.assertEqual(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            j[0]["sha256"],
        )
        self.assertEqual(response.headers.get("x-azul-security"), "LOW TLP:AMBER")
        # check that dispatcher would have submitted event with our content and alt streams
        event: azm.BinaryEvent = se.call_args[0][0][0]
        self.assertEqual(
            [x.label for x in event.entity.datastreams],
            [
                azm.DataLabel.CONTENT.value,
                azm.DataLabel.TEST.value,
                azm.DataLabel.TEXT.value,
                azm.DataLabel.DEOB_JS.value,
            ],
        )
        self.assertEqual(
            [x.sha256 for x in event.entity.datastreams],
            [
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                "5b41362bc82b7f3d56edc5a306db22105707d01ff4819e26faef9724a2d406c9",
                "d98cf53e0c8b77c14a96358d5b69584225b4bb9026423cbc2f7b0161894c402c",
                "f60f2d65da046fcaaf8a10bd96b5630104b629e111aff46ce89792e1caa11b18",
            ],
        )

    async def test_obj_more_secure_than_user(self):
        datab = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "user")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "banana"}))),
        ]

        # User doesn't have inclusive
        data2 = copy.copy(datab) + [("security", (None, "rel:bee"))]
        response = self.client.post("/v0/binaries/source", files=data2 + [("binary", ("file.exe", b"hello"))])
        self.assertEqual(422, response.status_code)

        # User doesn't have exclusive
        data2 = copy.copy(datab) + [("security", (None, "medium"))]
        response = self.client.post("/v0/binaries/source", files=data2 + [("binary", ("file.exe", b"hello"))])
        self.assertEqual(422, response.status_code)

        # User doesn't have inclusive or exclusive
        data2 = copy.copy(datab) + [("security", (None, "rel:bee"))]
        response = self.client.post("/v0/binaries/source", files=data2 + [("binary", ("file.exe", b"hello"))])
        self.assertEqual(422, response.status_code)

    async def test_cart(self):
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "user")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "banana"}))),
            ("security", (None, "low")),
        ]
        # upload cart
        istream = io.BytesIO(b"hello")
        ostream = io.BytesIO()
        cart.pack_stream(istream, ostream)
        fdata = ostream.getvalue()

        response = self.client.post("/v0/binaries/source", files=data + [("binary", ("file.exe", fdata))])
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        self.assertEqual(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            j[0]["sha256"],
        )
        self.assertEqual(response.headers.get("x-azul-security"), "LOW")

    async def test_malpz(self):
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "user")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "banana"}))),
            ("security", (None, "low")),
        ]
        # upload malpz
        fdata = malpz.wrap(b"hello", classification="")
        response = self.client.post("/v0/binaries/source", files=data + [("binary", ("file.exe", fdata))])
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        self.assertEqual(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            j[0]["sha256"],
        )
        self.assertEqual(response.headers.get("x-azul-security"), "LOW")

    async def test_zip(self):
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "user")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "banana"}))),
            ("security", (None, "low")),
        ]
        # upload zip
        ostream = io.BytesIO()
        with pyzipper.ZipFile(ostream, "w", compression=pyzipper.ZIP_DEFLATED) as zf:
            zf.writestr("file.txt", b"hello")
        fdata = ostream.getvalue()
        response = self.client.post(
            "/v0/binaries/source",
            files=data + [("binary", ("file.exe", fdata))],
            params={"extract": True},
        )
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        self.assertEqual(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            j[0]["sha256"],
        )
        self.assertEqual(response.headers.get("x-azul-security"), "LOW")

    async def test_zip_password(self):
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "user")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "banana"}))),
            ("security", (None, "low")),
        ]
        # upload zip password
        with helpers.get_file(f"fake_infected.zip") as f:
            fdata = f.read()

        response = self.client.post(
            "/v0/binaries/source",
            files=data + [("binary", ("file.exe", fdata))],
            params={"extract": True, "password": "bad"},
        )
        self.assertEqual(422, response.status_code)

        response = self.client.post(
            "/v0/binaries/source",
            files=data + [("binary", ("file.exe", fdata))],
            params={"extract": True, "password": "infected"},
        )
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(2, len(j))
        self.assertEqual(
            "dac804f3662b2228e43af80f6e0769614bf53d6c8ea16241c80d779de1308c20",
            j[0]["sha256"],
        )
        # mocked dispatcher response so same hash
        self.assertEqual(
            "db169d56dee6ca92ffc84538b352702169a7705217b37520a00884ab43cf7317",
            j[1]["sha256"],
        )
        self.assertEqual(response.headers.get("x-azul-security"), "LOW")

    async def test_aes_zip_bad_password(self):
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "user")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "banana"}))),
            ("security", (None, "low")),
        ]
        # upload AES zipped file
        with helpers.get_file(f"aes_zip.zip") as f:
            fdata = f.read()

        response = self.client.post(
            "/v0/binaries/source",
            files=data + [("binary", ("aes_zip.exe", fdata))],
            params={"extract": True, "password": "wrongPassword"},
        )
        self.assertEqual(422, response.status_code)
        self.assertEqual(response.headers.get("x-azul-security"), "LOW")

    async def test_aes_zip(self):
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "user")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "banana"}))),
            ("security", (None, "low")),
        ]
        # upload AES zipped file
        with helpers.get_file(f"aes_zip.zip") as f:
            fdata = f.read()
        response = self.client.post(
            "/v0/binaries/source",
            files=data + [("binary", ("aes_zip.exe", fdata))],
            params={"extract": True, "password": "infected"},
        )
        print(response.content)
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        self.assertEqual(
            "5b3c6cfc185d6eac904ae716c99cf1155d58f3b7e818d6ab2ecfea3e05a7a0bb",
            j[0]["sha256"],
        )
        self.assertEqual(response.headers.get("x-azul-security"), "LOW")

    async def test_zip_subfolders(self):
        # upload zip with multiple sub folders
        data = [
            ("filename", (None, "test.exe")),
            ("source_id", (None, "user")),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("references", (None, json.dumps({"apple": "banana"}))),
            ("security", (None, "low")),
        ]
        zipData = ""
        with helpers.get_file(f"multi_sub_folder.zip") as f:
            zipData = f.read()

        response = self.client.post(
            "/v0/binaries/source",
            files=data + [("binary", ("multi_sub_folder.zip", zipData))],
            params={"extract": True},
        )
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(4, len(j))
        self.assertEqual(
            "2309f4528245261321cf2646823bcf70dc855dd2fde09a0f8d27229db5e0c00e",
            j[0]["sha256"],
        )
        self.assertEqual(
            "21f0fe9425ce40c8ae188d4adff7e5e9fcdc8483b270e184a6f15c353b122a58",
            j[1]["sha256"],
        )
        self.assertEqual(
            "d58a0240ec393bd46f77bdcd3c963c9833c34409074d0508bf13fd824cd872aa",
            j[2]["sha256"],
        )
        self.assertEqual(
            "608eaf01f814c150c12fab5a213c1eff23dd86e28d1f5fd7b6a36e18c29ec3c0",
            j[3]["sha256"],
        )
        self.assertEqual(response.headers.get("x-azul-security"), "LOW")


# ----------------------------------------------------------------------------------------- Submit Source Dataless
@mock.patch("azul_bedrock.dispatcher.DispatcherAPI", unit_test.FakeDispatcherAPI)
@mock.patch.object(Context, "get_user_access", helpers.mock_get_user_access)
class TestSubmitSourceDataless(unit_test.DataMockingUnitTest):
    def setUp(self):
        super().setUp()
        self.data = b"data"
        self.data256 = hashlib.sha256(self.data).hexdigest()

    @mock.patch("azul_metastore.query.binary2.binary_submit_dataless.stream_dispatcher_events_for_binary")
    async def test_dataless_normal(self, sefe):
        # upload normal
        sefe.return_value = (
            x
            for x in [
                gen.binary_event(
                    {
                        "entity": json.loads(
                            binary_submit._transform_metadata_to_binary_entity(
                                gen.gen_binary_data(self.data),
                                "evil.exe",
                                augstreams=[],
                            ).model_dump_json()
                        )
                    },
                    model=False,
                )
            ]
        )
        response = self.client.post(
            "/v0/binaries/source/dataless",
            files=[
                ("filename", (None, "test.exe")),
                ("source_id", (None, "user")),
                ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
                ("references", (None, json.dumps({"apple": "banana"}))),
                ("security", (None, "low TLP:CLEAR TLP:GREEN TLP:AMBER")),
                ("sha256", (None, self.data256)),
            ],
        )
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        self.assertEqual(self.data256, j[0]["sha256"])
        self.assertEqual(response.headers.get("x-azul-security"), "LOW TLP:AMBER")

    @mock.patch("azul_metastore.query.binary2.binary_submit_dataless.stream_dispatcher_events_for_binary")
    async def test_dataless_normal_no_filename(self, sefe):
        sefe.return_value = (
            x
            for x in [
                gen.binary_event(
                    {
                        "entity": json.loads(
                            binary_submit._transform_metadata_to_binary_entity(
                                gen.gen_binary_data(self.data),
                                "evil.exe",
                                augstreams=[],
                            ).model_dump_json()
                        )
                    },
                    model=False,
                )
            ]
        )
        response = self.client.post(
            "/v0/binaries/source/dataless",
            files=[
                ("source_id", (None, "user")),
                ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
                ("references", (None, json.dumps({"apple": "banana"}))),
                ("security", (None, "low TLP:CLEAR TLP:GREEN TLP:AMBER")),
                ("sha256", (None, self.data256)),
            ],
        )
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        self.assertEqual(self.data256, j[0]["sha256"])

    @mock.patch("azul_metastore.query.binary2.binary_submit_dataless.stream_dispatcher_events_for_binary")
    async def test_post_dataless_missing(self, sefe):
        # upload missing
        sefe.return_value = (x for x in [])
        response = self.client.post(
            "/v0/binaries/source/dataless",
            files=[
                ("filename", (None, "test.exe")),
                ("source_id", (None, "user")),
                ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
                ("references", (None, json.dumps({"apple": "banana"}))),
                ("security", (None, "low TLP:CLEAR TLP:GREEN TLP:AMBER")),
                ("sha256", (None, self.data256)),
            ],
        )
        self.assertEqual(404, response.status_code)


# ----------------------------------------------------------------------------------------- Submit Child
@mock.patch("azul_bedrock.dispatcher.DispatcherAPI", unit_test.FakeDispatcherAPI)
@mock.patch.object(Context, "get_user_access", helpers.mock_get_user_access)
class TestSubmitChild(unit_test.DataMockingUnitTest):
    async def test_parent_normal(self):
        data = [
            ("filename", (None, "test.exe")),
            ("parent_sha256", (None, hashlib.sha256(b"hello").hexdigest())),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "low TLP:CLEAR TLP:GREEN TLP:AMBER")),
        ]
        response = self.client.post("/v0/binaries/child", files=data + [("binary", ("file.exe", b"hello"))])
        print(response.content)
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        self.assertEqual(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            j[0]["sha256"],
        )

    async def test_parent_normal_no_filename(self):
        data = [
            ("parent_sha256", (None, hashlib.sha256(b"hello").hexdigest())),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "low TLP:CLEAR TLP:GREEN TLP:AMBER")),
        ]
        response = self.client.post("/v0/binaries/child", files=data + [("binary", ("file.exe", b"hello"))])
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        self.assertEqual(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            j[0]["sha256"],
        )

    async def test_parent_cart(self):
        data = [
            ("parent_sha256", (None, hashlib.sha256(b"hello").hexdigest())),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "low TLP:CLEAR TLP:GREEN TLP:AMBER")),
        ]

        # upload cart
        istream = io.BytesIO(b"hello")
        ostream = io.BytesIO()
        cart.pack_stream(istream, ostream)
        fdata = ostream.getvalue()

        response = self.client.post("/v0/binaries/child", files=data + [("binary", ("file.exe", fdata))])
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        self.assertEqual(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            j[0]["sha256"],
        )


# ----------------------------------------------------------------------------------------- Submit Child Dataless
@mock.patch("azul_bedrock.dispatcher.DispatcherAPI", unit_test.FakeDispatcherAPI)
@mock.patch.object(Context, "get_user_access", helpers.mock_get_user_access)
class TestSubmitChildDataless(unit_test.DataMockingUnitTest):
    def setUp(self):
        super().setUp()
        self.parent_data = b"data"
        self.parent_sha256 = hashlib.sha256(self.parent_data).hexdigest()
        self.child_data = b"childdatahello"
        self.child_sha256 = hashlib.sha256(self.child_data).hexdigest()

    @mock.patch("azul_metastore.query.binary2.binary_submit_dataless.stream_dispatcher_events_for_binary")
    async def test_parent_normal(self, sefe):
        sefe.return_value = (
            x
            for x in [
                gen.binary_event(
                    {
                        "entity": json.loads(
                            binary_submit._transform_metadata_to_binary_entity(
                                gen.gen_binary_data(self.child_data),
                                "evil.exe",
                                augstreams=[],
                            ).model_dump_json()
                        )
                    },
                    model=False,
                )
            ]
        )
        data = [
            ("sha256", (None, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")),
            ("filename", (None, "test.exe")),
            ("parent_sha256", (None, self.parent_sha256)),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "low TLP:CLEAR TLP:GREEN TLP:AMBER")),
        ]
        response = self.client.post("/v0/binaries/child/dataless", files=data)
        print(response.content)
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        self.assertEqual(
            self.child_sha256,
            j[0]["sha256"],
        )

    @mock.patch("azul_metastore.query.binary2.binary_submit_dataless.stream_dispatcher_events_for_binary")
    async def test_parent_normal_no_filename(self, sefe):
        sefe.return_value = (
            x
            for x in [
                gen.binary_event(
                    {
                        "entity": json.loads(
                            binary_submit._transform_metadata_to_binary_entity(
                                gen.gen_binary_data(self.child_data),
                                "evil.exe",
                                augstreams=[],
                            ).model_dump_json()
                        )
                    },
                    model=False,
                )
            ]
        )
        data = [
            ("sha256", (None, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")),
            ("parent_sha256", (None, self.parent_sha256)),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "low TLP:CLEAR TLP:GREEN TLP:AMBER")),
        ]
        response = self.client.post("/v0/binaries/child/dataless", files=data)
        self.assertEqual(200, response.status_code)
        j = json.loads(response.text)
        self.assertEqual(1, len(j))
        self.assertEqual(
            self.child_sha256,
            j[0]["sha256"],
        )

    @mock.patch("azul_metastore.query.binary2.binary_submit_dataless.stream_dispatcher_events_for_binary")
    async def test_parent_missing(self, sefe):
        sefe.return_value = (x for x in [])

        data = [
            ("sha256", (None, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")),
            ("filename", (None, "test.exe")),
            ("parent_sha256", (None, self.parent_sha256)),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "low TLP:CLEAR TLP:GREEN TLP:AMBER")),
        ]

        response = self.client.post("/v0/binaries/child/dataless", files=data)
        self.assertEqual(404, response.status_code)

    @mock.patch("azul_metastore.query.binary2.binary_submit_dataless.stream_dispatcher_events_for_binary")
    async def test_tlp_amber_strict(self, sefe):
        sefe.return_value = (x for x in [])

        data = [
            ("sha256", (None, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")),
            ("filename", (None, "test.exe")),
            ("parent_sha256", (None, self.parent_sha256)),
            ("timestamp", (None, "2020-06-02 11:47:03.2Z")),
            ("security", (None, "low TLP:CLEAR TLP:GREEN TLP:AMBER+STRICT")),
        ]

        # User doesn't have TLP:AMBER+STRICT so can't upload the file and gets denied based on their permissions.
        response = self.client.post("/v0/binaries/child/dataless", files=data)
        self.assertEqual(422, response.status_code)
