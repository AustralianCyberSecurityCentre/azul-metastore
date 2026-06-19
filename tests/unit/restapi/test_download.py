import json
from io import BytesIO

from pydantic import BaseModel

from tests.support import unit_test
from azul_metastore.settings import get as get_metastore_settings


class TestDownloadHashes(unit_test.BaseUnitTestCase):
    async def test_simple_download(self):
        """Download event should fail when readonly mode is set."""
        get_metastore_settings().readonly_mode = True
        request_data = {
            "sha256": "testsha256",
            "source_id": "samples",
            "references": {"apple": "granny smith"},
            "security": "LOW",
        }
        response = self.client.post("/v0/binaries/source/download", json=request_data)
        self.assertEqual(423, response.status_code)
        j = json.loads(response.text)
        print(j["detail"]["internal"])
        self.assertEqual(j["detail"]["internal"], ExceptionCodeEnum.MetastoreReadOnlyMode.value)
