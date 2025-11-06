import unittest
import unittest.async_case
from typing import AsyncIterable

import black
from azul_bedrock import models_api
from azul_bedrock import models_network as azm
from azul_bedrock.dispatcher import DispatcherAPI
from starlette.datastructures import UploadFile

from tests.support import gen


def resp_submit_events(
    events: list[azm.BaseEvent],
    *,
    sync: bool = False,
    include_ok: bool = False,
    raise_invalid: bool = True,
    model: str,
    params: dict | None = None,
) -> models_api.ResponsePostEvent:
    ret = []
    ok = [x.model_dump() for x in events]
    # hack code to emulate golang generation
    for orig in ok:
        if model == azm.ModelType.Binary:
            ev = gen.add_binary_tracking(azm.BinaryEvent(**orig)).model_dump()
        elif model == azm.ModelType.Insert:
            ev = gen.add_insert_tracking(azm.InsertEvent(**orig)).model_dump()
        else:
            ev = orig
        ret.append(ev)

    return models_api.ResponsePostEvent(total_ok=len(ret), total_failures=0, failures=[], ok=ret)


async def resp_async_submit_binary(
    source: str, label: azm.DataLabel, content: UploadFile | AsyncIterable[bytes], timeout: float | None = None
):
    """Return fake dispatcher response for posting data asynchronously."""
    data = b""
    async for d in DispatcherAPI.async_convert_to_async_iterable(content):
        data += d
    return gen.gen_binary_data(data, label=label)


class BasicTest(unittest.async_case.IsolatedAsyncioTestCase):
    """Provide basic helpers for unit tests."""

    maxDiff = None

    def assertFormatted(self, in1, in2):
        """Format input repr() as multiline strings so unittest diff works on objects.

        This requires the input repr() to be valid python code and is intended for use with
        pydantic models.
        """
        try:
            self.assertEqual(in1, in2)
        except AssertionError:
            print(
                "The below printout can form the base of your test, "
                "but you must double check the output is as expected.\n"
            )
            print(repr(in1))
            print("")
            # use black to render the difference between the two
            # exploiting the fact that the repr() is valid python code
            string1 = black.format_str(repr(in1), mode=black.Mode())
            string2 = black.format_str(repr(in2), mode=black.Mode())
            self.assertEqual(string1, string2)
