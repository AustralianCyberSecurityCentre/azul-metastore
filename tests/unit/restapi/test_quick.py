import json
from io import BytesIO

from pydantic import BaseModel

from tests.support import unit_test


class PydanticTestModel(BaseModel):
    important_field_a: bool
    important_field_b: str


class TestQuickRefs(unit_test.BaseUnitTestCase):
    def _extract_state(self, response, check_types) -> dict:
        # Extract depending on the state
        # FastAPI encodes types by default with Pydantic, but sometimes we skip doing this
        # in our application for performance reasons
        # Handle both cases
        if isinstance(response, dict):
            self.assertTrue(check_types)

            data = response["data"]
        else:
            self.assertFalse(check_types)

            decoded_data = json.load(BytesIO(response.body))
            data = decoded_data["data"]

        return data
