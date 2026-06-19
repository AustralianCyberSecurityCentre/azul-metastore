import json
from io import BytesIO

from pydantic import BaseModel

from tests.support import unit_test
from azul_metastore.settings import get as get_metastore_settings


class TestStatistics(unit_test.BaseUnitTestCase):
    async def test_server_readonly(self):
        """Basic check to verify the server returns when it's in a readonly state and when it's not."""
        get_metastore_settings().readonly_mode = True
        response = self.client.get("/v0/readonly")
        self.assertEqual(200, response.status_code)
        self.assertEqual("true", response.text)

        get_metastore_settings().readonly_mode = False
        response = self.client.get("/v0/readonly")
        self.assertEqual(200, response.status_code)
        self.assertEqual("false", response.text)
