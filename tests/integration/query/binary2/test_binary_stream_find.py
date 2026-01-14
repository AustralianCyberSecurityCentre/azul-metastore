from azul_bedrock import models_network as azm

from azul_metastore.query.binary2 import binary_read
from tests.support import gen, integration_test


class TestStream(integration_test.DynamicTestCase):

    def test_find(self):
        self.write_binary_events([gen.binary_event(eid="e1")])

        # test found stream
        st = binary_read.find_stream_metadata(self.writer, "e1", "e1")
        expected = azm.Datastream(
            label="content",
            magic="ASCII text",
            mime="text/plain",
            file_format="text/plain",
            file_extension="txt",
            size=1024,
            tlsh="T10000000000000000000000000000000000000000000000000000000000000000000000",
            ssdeep="1:1:1",
            md5=f"{'e1':0>32}",
            sha1=f"{'e1':0>40}",
            sha256="e1",
            sha512=f"{'e1':0>128}",
            identify_version=1,
        )
        self.assertEqual(("generic_source", expected), st)

        # test found stream case sensitive
        st = binary_read.find_stream_metadata(self.writer, "E1", "E1")
        expected = azm.Datastream(
            label="content",
            magic="ASCII text",
            mime="text/plain",
            file_format="text/plain",
            file_extension="txt",
            size=1024,
            tlsh="T10000000000000000000000000000000000000000000000000000000000000000000000",
            ssdeep="1:1:1",
            md5=f"{'e1':0>32}",
            sha1=f"{'e1':0>40}",
            sha256="e1",
            sha512=f"{'e1':0>128}",
            identify_version=1,
        )
        self.assertEqual(("generic_source", expected), st)

        # test missing stream
        source, st = binary_read.find_stream_metadata(self.writer, "e1", "e10")
        self.assertTrue(st is None)

        # test stream for wrong entity
        source, st = binary_read.find_stream_metadata(self.writer, "invalid1", "e1")
        self.assertTrue(st is None)

    def test_find_augmented(self):
        ev = gen.binary_event(eid="e1", action=azm.BinaryAction.Augmented)
        ev.entity.datastreams.append(gen.data({"label": "cape_report"}, hash="v10"))
        self.write_binary_events([ev])

        # test content is not available
        st = binary_read.find_stream_metadata(self.writer, "e1", "e1")
        self.assertEqual((None, None), st)

        # test alt stream is available
        st = binary_read.find_stream_metadata(self.writer, "e1", "v10")
        expected = azm.Datastream(
            label="cape_report",
            magic="ASCII text",
            mime="text/plain",
            file_format="text/plain",
            file_extension="txt",
            size=1024,
            tlsh="T10000000000000000000000000000000000000000000000000000000000000000000000",
            ssdeep="1:1:1",
            md5=f"{'v10':0>32}",
            sha1=f"{'v10':0>40}",
            sha256="v10",
            sha512=f"{'v10':0>128}",
            identify_version=1,
        )
        self.assertEqual(("generic_source", expected), st)
