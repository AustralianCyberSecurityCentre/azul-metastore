"""Download all events from metastore that match the filter.

Restapi environment variables will need to be set appropriately.
"""

import contextlib
import hashlib
import json

from azul_bedrock.models_auth import UserInfo

from azul_metastore import context, settings
from azul_metastore.common import search_data
from azul_metastore.encoders import binary2 as rc

MAX_TO_SCAN = 10_000_000


def md5(text: str):
    """Return string md5 representing incoming text."""
    return hashlib.md5(text.encode()).hexdigest()  # noqa: S303 # nosec B303, B324


def get_ctx(creds: dict) -> context.Context:
    """Create context using creds."""
    user_info = UserInfo(username=creds["unique"], unique_id=creds["unique"])
    sd = search_data.SearchData(credentials=creds, security_exclude=[])
    return context.get_general_context().copy_with(user_info=user_info, sd=sd)


def get_events(dest: str, filters: list, must_not: list, max_entries: int = MAX_TO_SCAN) -> None:
    """Download any events matching filters."""
    print(f"downloading events for {dest}")
    ctx = get_ctx(settings.get_writer_creds())
    body = {
        # not downloading features increases performance
        "_source": {"include": ["source.name", "source.timestamp", "source.path"]},
        "query": {"bool": {"filter": filters, "must_not": must_not}},
    }
    with open(f"./{dest}.jsonlines", "w") as f, contextlib.suppress(KeyboardInterrupt):
        for i, resp in enumerate(ctx.man.binary2.w.scan(ctx.sd, body=body)):
            if i > 0 and i % 10000 == 0:
                print(f"{i} events downloaded")
            resp["_source"]["kafka_key"] = resp["_id"]
            try:
                event = rc.Binary2.decode(resp["_source"])
            except Exception:
                continue
            f.write(json.dumps(event))
            f.write("\n")
            if i > max_entries:
                break
    print(f"downloaded {i} events")


def main():
    """Main."""
    print("start")
    # sample of deep events
    get_events(
        "representative_sample_b2",
        [
            {"prefix": {"entity.sha256": {"value": "b"}}},
        ],
        [{"term": {"source.name": "virustotal"}}],
    )
    print("end")


if __name__ == "__main__":
    main()
