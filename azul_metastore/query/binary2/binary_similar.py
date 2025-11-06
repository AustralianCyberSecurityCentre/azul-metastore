"""Queries for finding similar binaries."""

from ctypes import CDLL, create_string_buffer
from typing import Iterable

import pendulum

from azul_metastore.common.tlsh import encode_tlsh_into_vector, strip_tlsh_version
from azul_metastore.context import Context
from azul_metastore.query import cache


def ssdeep_compare(hash1: str, hash2: str) -> int:
    """Compare 2 ssdeep fuzzyhashes and return a score for similarity.

    Uses libfuzzy-dev library
    """
    try:
        score = CDLL("libfuzzy.so").fuzzy_compare(
            create_string_buffer(hash1.encode("ascii")),
            create_string_buffer(hash2.encode("ascii")),
        )
        return score
    except OSError:
        raise Exception("could not find libfuzzy-dev; check that it is installed.")


def read_similar_from_tlsh(ctx: Context, tlsh: str, maxCount: int) -> list[dict]:
    """Compares binaries in OpenSearch by TLSH.

    This uses the kNN binary vector searching algorithm to find similar TLSH hashes by bit difference.
    """
    search_hash = encode_tlsh_into_vector(tlsh)

    # Normalise the hash to eliminate TLSH exact matches
    without_version = strip_tlsh_version(tlsh)

    body = {
        "_source": {"includes": ["sha256", "tlsh", "tlsh_vector"]},
        "query": {
            "knn": {
                "tlsh_vector": {
                    "vector": search_hash,
                    "min_score": 0.9,
                    # Requires OpenSearch 2.4+
                    # https://opensearch.org/docs/latest/vector-search/filter-search-knn/efficient-knn-filtering/
                    "filter": {
                        "bool": {
                            "must_not": [
                                # Don't match identical TLSHes
                                # FUTURE: We might want to change this to match on the entities SHA256.
                                # In testing, TLSH collisions were encountered for files which otherwise had
                                # different SHA256s, and identifying these files seems to make good sense (as
                                # they likely only have a byte or two difference)
                                {"match": {"tlsh": without_version}},
                                {"match": {"tlsh": "T1" + without_version}},
                            ],
                        }
                    },
                }
            }
        },
        "size": maxCount,
        # we only care about unique entity ids
        "collapse": {"field": "sha256"},
    }

    resp = ctx.man.binary2.w.search(ctx.sd, body=body)

    similar_hashes: list[dict] = []

    for hit in resp["hits"]["hits"]:
        # OpenSearch natively provides the search score for kNN
        # TLSH's scoring algorithm doesn't work here as it requires byte for byte comparisons, so we rely on
        # the bit differences provided by OpenSearch

        # knn call in wrapper inserts security as filter, but this affects the score by adding whole numbers
        # modulus removes these whole numbers so we get a useable percentage score
        # multipy by 100 to get a nice percentage
        # round to 2 decimal places because score can subtly vary
        score = round((hit["_score"] % 1) * 100, 2)

        similar_hashes.append({"sha256": hit["_source"]["sha256"], "score": score})

    # sort hashes by score
    similar_hashes.sort(key=lambda x: x["score"], reverse=True)

    # limit to maxCount results
    if len(similar_hashes) > maxCount:
        similar_hashes = similar_hashes[:maxCount]

    return similar_hashes


def read_similar_from_ssdeep(ctx: Context, fuzzyHash: str, maxCount: int) -> list[dict]:
    """Compares binaries in Opensearch by ssdeep fuzzyhash.

    ssdeep hashes are split into 3 colon-delimited parts: the block size, the hash for blocksize
    and the hash for blocksize*2.

    chunk and dchunk are the second and third parts of the hash, each split into ngrams 7 characters long.

    The opensearch query retreives entities which have the same blocksize give or take a multiple of 2,
    and have at least 1 ngram matching.

    These hashes are then passed to the fuzzyhash library to rank similarity on a 0-100 scale.

    A maximum of maxCount*2 entities will be fetched from Opensearch to account for any differences in ordering
    between the Opensearch results and the ssdeep comparision scoring, however only maxCount matches are returned.

    A list of sha256 hashes and corresponding similarity score is returned, ordered by score descending.
    """
    try:
        blockSizeStr, chunk, doubleChunk = fuzzyHash.split(":")
        blockSize = int(blockSizeStr)
    except ValueError:
        raise Exception(f"ssdeep could not be parsed {fuzzyHash}")

    body = {
        "_source": {"includes": ["sha256", "ssdeep"]},
        "query": {
            "bool": {
                "must": {
                    "bool": {
                        "should": [
                            {"match": {"encoded_ssdeep.chunk": chunk}},
                            {"match": {"encoded_ssdeep.dchunk": chunk}},
                            {"match": {"encoded_ssdeep.chunk": doubleChunk}},
                            {"match": {"encoded_ssdeep.dchunk": doubleChunk}},
                        ]
                    }
                },
                "must_not": [{"match": {"ssdeep": fuzzyHash}}],
                "filter": [{"terms": {"encoded_ssdeep.blocksize": [blockSize, blockSize * 2, blockSize / 2]}}],
            }
        },
        "size": maxCount * 2,
        # we only care about unique entity ids
        "collapse": {"field": "sha256"},
    }

    resp = ctx.man.binary2.w.search(ctx.sd, body=body)

    similarHashes: list[dict] = []

    for hit in resp["hits"]["hits"]:
        # calculate score
        score = ssdeep_compare(fuzzyHash, hit["_source"]["ssdeep"])
        similarHashes.append({"sha256": hit["_source"]["sha256"], "score": score})

    # sort hashes by score
    similarHashes.sort(key=lambda x: x["score"], reverse=True)

    # limit to maxCount results
    if len(similarHashes) > maxCount:
        similarHashes = similarHashes[:maxCount]

    return similarHashes


def read_similar_from_features(
    ctx: Context, sha256: str, *, recalculate: bool = False, only_cache: bool = False
) -> Iterable[dict]:
    """Find similar entities based on features.

    Ignores special parsed values for features like uri host, path, etc.

    We have only the results of specific plugins rather than a super document with every feature for an entity...

    Given an entity, we look at each produced result and try to find similar documents in opensearch.
    For each entity id in all results, we sum scores together.
    """
    sha256 = sha256.lower()
    ret = cache.load_generic(ctx, "similar", sha256, "v2")
    if (not recalculate and ret) or only_cache:
        yield ret
        yield ret
        return

    ret = {
        "num_feature_values": 0,
        "timestamp": pendulum.now(tz=pendulum.UTC).to_iso8601_string(),
        "matches": [],
        "status": "starting",
    }
    yield ret

    def _cache():
        cache.store_generic(ctx, "similar", sha256, "v2", ret)

    ret["status"] = "getting binary feature values"
    _cache()

    body = {
        "query": {
            "bool": {
                "filter": [
                    {"range": {"num_feature_values": {"gt": 4}}},
                    {"term": {"sha256": sha256}},
                ]
            }
        },
        "collapse": {"field": "uniq_features"},
        "_source": {
            "includes": [
                "features.value",
                "features.name",
                "author.name",
                "sha256_author_action",
            ]
        },
        "size": 1000,
    }
    resp = ctx.man.binary2.w.search(ctx.sd, body=body, routing=sha256)

    # for each result that the entity has, look for similar documents
    author_docs: dict[str, dict] = {}
    author_features: dict[str, set[str]] = {}
    author_feature_vals: dict[str, set[tuple[str, str]]] = {}
    num_fvals = 0
    for doc in resp["hits"]["hits"]:
        set_f = set()
        set_fval = set()
        for x in doc["_source"]["features"]:
            set_f.add(x["name"])
            set_fval.add((x["name"], x["value"]))

        author = doc["_source"]["author"]["name"]
        author_docs.setdefault(author, []).append(
            {"_index": doc["_index"], "_id": doc["_id"], "routing": doc["_routing"]}
        )
        author_features.setdefault(author, set()).update(set_f)
        author_feature_vals.setdefault(author, set()).update(set_fval)
        num_fvals += len(set_fval)
    ret["num_feature_values"] = num_fvals

    map_entity: dict[str, dict] = {}
    for i, author in enumerate(author_docs.keys()):
        set_fval = author_feature_vals[author]
        ret["status"] = f"({i + 1}/{len(author_docs)}) calculating matches for {author}"
        _cache()
        body = {
            "timeout": "20s",
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"num_feature_values": {"gt": 4}}},
                        {"term": {"author.name": author}},
                        {
                            "more_like_this": {
                                "fields": [f"features_map.{x}" for x in author_features[author]],
                                "like": author_docs[author],
                                "min_term_freq": 1,
                                "min_doc_freq": 1,
                                # only look at x feature values in the result
                                "max_query_terms": 1000,
                                # at least 4 feature values in common for an author
                                "minimum_should_match": 4,
                            }
                        },
                    ],
                    "must_not": [{"term": {"sha256": sha256}}],
                }
            },
            "size": 1000,
            "collapse": {"field": "sha256"},
            "_source": {
                "includes": [
                    "sha256",
                    "features.name",
                    "features.value",
                    "sha256",
                ]
            },
        }
        resp = ctx.man.binary2.w.search(ctx.sd, body=body)
        for row in resp["hits"]["hits"]:
            doc = row["_source"]
            # calculate number of matching feature values to target
            collision = len(set_fval.intersection(set((x["name"], x["value"]) for x in doc["features"])))

            # create new match for entity if it doesnt exist
            map_entity.setdefault(
                doc["sha256"],
                dict(
                    sha256=doc["sha256"],
                    contributions={},
                    score_sum=0,
                ),
            )
            res = map_entity[doc["sha256"]]
            res["contributions"].setdefault(author, 0)
            res["contributions"][author] = max(collision, res["contributions"][author])

    # order contributions as a list & calculate sum score
    for row in map_entity.values():
        row["contributions"] = [(x, y) for x, y in row["contributions"].items()]
        row["score_sum"] = sum([x[1] for x in row["contributions"]])

    rows = sorted(map_entity.values(), key=lambda x: x["score_sum"], reverse=True)

    # get percentage match
    for row in rows:
        row["score_percent"] = int((row["score_sum"] / num_fvals) * 100)

    # sort by score_sum and sha256
    rows.sort(key=lambda x: x["sha256"])
    rows.sort(key=lambda x: x["score_sum"], reverse=True)
    rows = [x for x in rows if (x["score_sum"] > 1 or x["score_percent"] > 1)]

    # sort contributions by score
    [x["contributions"].sort(key=lambda x: x[1], reverse=True) for x in rows]

    # keep most similar
    ret["matches"] = rows[:20]
    ret["status"] = "complete"
    _cache()
    yield ret
