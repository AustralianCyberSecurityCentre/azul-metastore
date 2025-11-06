"""Generate worst case for old vs new indexing comparison.

In this scenario, we are dealing with a very common file extracted from many different files.
This means the near-same event is seen with identical features and info thousands of times.

Beware, this is specifically designed to make the experimental ingestor look good.

The experimental ingestor will perform worse than stable when seeing entirely unique data
 due to the additional events indexed.

In testing with 1 node local opensearch cluster:
9000 of 9000
indexing old: 8.84s (9000) new: 3.74 (6004) ratio: 42.27%

The new method is 60% faster and adds ~3000 less docs to opensearch.
3000 of the 6004 added docs are stubs with a sha256 in them (parent doc).

Data usage in opensearch:
yellow open azul.x.local.binary.testing.2002 <index-id-A> 3 2 30000 4257  77.3mb  77.3mb
yellow open azul.x.local.binary2.b           <index-id-B> 3 2 20000  700    13mb    13mb
yellow open azul.x.local.binary2.d           <index-id-C> 3 2     4    0 116.1kb 116.1kb

azul.x.local.binary.testing.2002 is the old method and uses 77mb.
The other two indices are the new method and use a combined 13.2mb (82% disk usage saving).
"""

import os
import sys

# allow for loading tests module
DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.abspath(os.path.join(DIR, "../")))


from azul_bedrock import models_network as azm  # noqa: E402

from azul_metastore import context  # noqa: E402
from azul_metastore.query import binary_create  # noqa: E402
from azul_metastore.restapi.quick import qr  # noqa: E402
from tests.support import gen  # noqa: E402

# Aligning this with the remote environment.
gen.g1_1, gen.g2_1, gen.g3_1, gen.g1_12, gen.g2_12, gen.g3_12 = (
    "LESS OFFICIAL TLP:CLEAR",
    "OFFICIAL REL:APPLE",
    "OFFICIAL MOD1 REL:APPLE",
    "LESS OFFICIAL TLP:GREEN",
    "OFFICIAL REL:APPLE REL:BEE",
    "OFFICIAL MOD1 REL:APPLE REL:BEE",
)

WORST_CASE_ID = "dd0"


def _sourced(eid):
    return gen.binary_event(
        eid=eid,
        sourceit=("testing", "2002-01-01T01:01:01Z"),
        authornv=("submitter", "1"),
        spath=[gen.path(eid=eid, authoru="submitter", action=azm.BinaryAction.Sourced)],
        action=azm.BinaryAction.Sourced,
    )


def _extracted(parent_eid):
    return gen.binary_event(
        eid=WORST_CASE_ID,
        sourceit=("testing", "2002-01-01T01:01:01Z"),
        authornv=("extractor", "1"),
        spath=[
            gen.path(eid=parent_eid, authoru="submitter", action=azm.BinaryAction.Sourced),
            gen.path(eid=WORST_CASE_ID, authoru="extractor", action=azm.BinaryAction.Extracted),
        ],
        action=azm.BinaryAction.Extracted,
        features=gen_features(3),
    )


def _enriched(parent_eid):
    return gen.binary_event(
        eid=WORST_CASE_ID,
        sourceit=("testing", "2002-01-01T01:01:01Z"),
        authornv=("enricher", "1"),
        spath=[
            gen.path(eid=parent_eid, authoru="submitter", action=azm.BinaryAction.Sourced),
            gen.path(eid=WORST_CASE_ID, authoru="extractor", action=azm.BinaryAction.Extracted),
            gen.path(eid=WORST_CASE_ID, authoru="enricher", action=azm.BinaryAction.Enriched),
        ],
        features=gen_features(10),
        action=azm.BinaryAction.Enriched,
        info={"thing": "data" * 100},
    )


def generate_tree(total_rounds: int):
    """Generate relationship tree with many relationships to a single binary."""
    for i in range(total_rounds):
        parent = f"bb{i}"
        # total experimental expected count = n*2
        yield _sourced(parent)
        # total experimental expected count = 2
        yield _extracted(parent)
        # total experimental expected count = 1
        yield _enriched(parent)


def gen_features(num_features: int):
    """Generate many features."""
    features = []
    for i in range(num_features):
        features.append(azm.FeatureValue(name="longname", value=f"thing{i}." * 50, type=azm.FeatureType.String))
    return features


def create_worst_case(ctx: context.Context):
    """Generate the worst case events for a binary."""
    MAX = 3_000
    collect = []
    for total, doc in enumerate(generate_tree(MAX)):
        collect.append(doc)
        if len(collect) >= 100:
            print(f"{total + 1} of {MAX * 3}")
            # time to create
            failed, _duplicates = binary_create.create_binary_events(ctx, collect)
            if failed:
                raise Exception("some failed")
            if _duplicates:
                raise Exception(f"some {_duplicates=}")
            collect = []
    failed, _duplicates = binary_create.create_binary_events(ctx, collect)
    if failed:
        raise Exception("some failed")
    if _duplicates:
        raise Exception(f"some {_duplicates=}")


def main():
    """Do main things."""
    ctx = qr.writer
    create_worst_case(ctx)


if __name__ == "__main__":
    main()
