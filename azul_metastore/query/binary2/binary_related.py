"""Queries for reading relationships between binaries."""

import logging

from azul_bedrock import models_network as azm
from azul_bedrock import models_restapi
from azul_bedrock.models_restapi import binaries as bedr_binaries

from azul_metastore.context import Context
from azul_metastore.encoders import binary2

logger = logging.getLogger(__name__)


def read_children(ctx: Context, sha256: str, detail: bool = False, bucket_size=100) -> list[bedr_binaries.PathNode]:
    """Return binary_link information about a specific binary."""
    sha256 = sha256.lower()
    body = {
        # usually caps at 10k but we want the full count sometimes
        "track_total_hits": detail if detail else 10000,
        # don't want individual docs
        "size": 0,
        # only aggregate on docs where eid is the parent
        "query": {
            "bool": {
                "filter": [
                    {"term": {"parent.sha256": sha256}},
                ],
            }
        },
        "aggs": {
            "CHILDREN": {
                "terms": {
                    "field": "sha256",
                    "size": bucket_size,
                    "order": {"NEWEST": "desc"},
                },
                "aggs": {
                    "HITS": {
                        "top_hits": {
                            "size": 1,
                            "_source": {"includes": binary2.fields_link + binary2.fields_recover_source_binary_node},
                        }
                    },
                    "NEWEST": {"max": {"field": "timestamp"}},
                },
            },
        },
    }
    resp = ctx.man.binary2.w.search(ctx.sd, body=body)
    children = []
    for bucket in resp["aggregations"]["CHILDREN"]["buckets"]:
        row = bucket["HITS"]["hits"]["hits"][0]["_source"]
        node = binary2.Binary2.recover_source_binary_node(row)
        node.pop("encoded", None)
        node["track_link"] = row["track_link"]
        children.append(bedr_binaries.PathNode(**node))
    return children


def _read_nearby_find_children(sha256s: set[str], index_alias: str, max_nodes: int) -> list[dict]:
    """Generate the body of an msearch query to get all children for the supplied sha256s."""
    sha256s = list(sha256s)
    if not sha256s:
        return []
    queries = []
    body = {
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {
                        "terms": {
                            "action": [
                                azm.BinaryAction.Sourced,
                                azm.BinaryAction.Extracted,
                                azm.BinaryAction.Mapped,
                            ]
                        }
                    },
                    # get child documents - matching parent sha256
                    {"terms": {"parent.sha256": sha256s}},
                ],
            }
        },
        "aggs": {
            # group by parent sha256
            "RELATED": {
                "terms": {"field": "parent.sha256", "size": len(sha256s)},
                "aggs": {
                    # group by child sha256
                    "RELATED": {
                        "terms": {"field": "sha256", "size": max_nodes},
                        "aggs": {
                            "HIT": {
                                "top_hits": {
                                    "size": 1,
                                    "_source": {"excludes": ["features", "datastreams", "info", "source"]},
                                    # Sort by size to prioritise events that have size and
                                    # file format information (sourced over mapped events)
                                    "sort": {"size": "asc", "file_format": "desc"},
                                }
                            }
                        },
                    }
                },
            }
        },
    }
    queries.append({"index": index_alias})
    queries.append(body)
    return queries


def _read_nearby_find_parents(sha256s: set[str], index_alias: str, max_nodes: int) -> list[dict]:
    """Generate the body of an msearch query to get all parents for the supplied sha256s."""
    sha256s = list(sha256s)
    if not sha256s:
        return []
    queries = []
    body = {
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    # ensure doc has a source
                    {
                        "terms": {
                            "action": [
                                azm.BinaryAction.Sourced,
                                azm.BinaryAction.Extracted,
                                azm.BinaryAction.Mapped,
                            ]
                        }
                    },
                    # get child documents - matching sha256
                    {"terms": {"sha256": sha256s}},
                ],
            }
        },
        "aggs": {
            # group by child sha256
            "RELATED": {
                "terms": {"field": "sha256", "size": len(sha256s)},
                "aggs": {
                    # group by parent sha256
                    "RELATED": {
                        "terms": {"field": "parent.sha256", "size": max_nodes},
                        "aggs": {
                            "HIT": {
                                "top_hits": {
                                    "size": 1,
                                    "_source": {"excludes": ["features", "datastreams", "info", "source"]},
                                    # Sort by size to prioritise events that have size and
                                    # file format information (sourced over mapped events)
                                    "sort": {"size": "asc", "file_format": "desc"},
                                }
                            }
                        },
                    },
                    # group by top-level source
                    "SOURCES": {
                        "filter": {"term": {"depth": 0}},
                        "aggs": {
                            "SOURCES": {
                                "terms": {"field": "source.name", "size": max_nodes},
                                "aggs": {
                                    "HIT": {
                                        "top_hits": {
                                            "size": 1,  # ensure only 1 source variant per node
                                            "_source": {"excludes": ["features", "datastreams", "info"]},
                                            # Sort by size to prioritise events that have size and
                                            # file format information (sourced over mapped events)
                                            "sort": {"size": "asc", "file_format": "desc"},
                                        }
                                    }
                                },
                            },
                        },
                    },
                },
            },
        },
    }
    queries.append({"index": index_alias})
    queries.append(body)
    return queries


def _read_nearby_process_resp(
    resp: dict, in_parents: set[str], in_children: set[str]
) -> tuple[dict[str, models_restapi.ReadNearbyLink], set[str], set[str], set[str]]:
    """Processes the response from a msearch looking for parents, children or both.

    Important the resp dictionary must be in the same order as the type_ids.

    Returns a tuple of [link info, set of parents, set of children, set of cousins]
    """
    unique_links: dict[str, models_restapi.ReadNearbyLink] = {}
    parents = set()
    children = set()
    cousins = set()
    for r in resp["responses"]:
        for rowUpper in r["aggregations"]["RELATED"]["buckets"]:
            if "SOURCES" in rowUpper:
                for row in rowUpper["SOURCES"]["SOURCES"]["buckets"]:
                    # only ever one hit
                    raw = row["HIT"]["hits"]["hits"][0]["_source"]
                    child_node = binary2.Binary2.recover_source_binary_node(raw)

                    source = raw["source"]
                    source["track_source_references"] = raw["track_source_references"]
                    source.pop("encoded_references", None)
                    source.pop("encoded_settings", None)
                    source = models_restapi.EventSource.model_validate(source)
                    sha256_author_action = raw["sha256_author_action"]
                    tmp_link = models_restapi.ReadNearbyLink(
                        id=sha256_author_action + source.name,  # Might not make sense for parent case
                        child=child_node["sha256"],
                        child_node=child_node,
                        source=source,
                    )
                    # If depth is 0 this is a submission to a source.
                    unique_links[tmp_link.id] = tmp_link

            for row in rowUpper["RELATED"]["buckets"]:
                # only ever one hit
                raw = row["HIT"]["hits"]["hits"][0]["_source"]
                child_node = binary2.Binary2.recover_source_binary_node(raw)

                # skip binaries that have themselves as parents
                # FUTURE assemblyline mapped creates a circular reference as of July 2025
                if raw["sha256"] == raw["parent"]["sha256"]:
                    continue

                # node has parent
                parent_node = raw["parent"]
                sha256_author_action = raw["sha256_author_action"]
                child_node.pop("encoded", None)
                tmp_link = models_restapi.ReadNearbyLink(
                    id=sha256_author_action + "." + parent_node["encoded"]["sha256_author_action"],
                    child=child_node["sha256"],
                    child_node=child_node,
                    parent=parent_node["sha256"],
                )
                if raw["sha256"] in in_parents:
                    # if the current node is a parent look for the parents, a parents child is a 'cousin/relative'.
                    parents.add(parent_node["sha256"])
                    cousins.add(child_node["sha256"])
                elif parent_node["sha256"] in in_children:
                    # Node is a child of the original node or a child of it's children,
                    # add to children and the parents of those nodes are 'cousins/relatives'
                    children.add(child_node["sha256"])
                    cousins.add(parent_node["sha256"])
                else:
                    # In the case of a 'cousin/relative' add parents and children as 'cousin/relative'.
                    cousins.add(parent_node["sha256"])
                    cousins.add(child_node["sha256"])

                unique_links[tmp_link.id] = tmp_link

    # If it's in parent/child it's not a cousin so remove it.
    cousins = cousins.difference(parents)
    cousins = cousins.difference(children)
    return unique_links, parents, children, cousins


def read_nearby(
    ctx: Context,
    sha256: str,
    include_cousins: bool = False,
    max_cousins: int = 100,
    max_cousin_distance: int = 2,
) -> models_restapi.ReadNearby:
    """Return nearby nodes and paths between them.

    Note - cousins is actually any child or child of childs, parent and any parent or parent of parents, child
    and any parent or parent of parents, child.
    So technically siblings, cousins, step children (child that has 20 parents including target node) etc.
    """
    sha256 = sha256.lower()
    # Do an initial search for all children and parents for the starting node.
    searches = _read_nearby_find_children([sha256], ctx.man.binary2.w.alias, max_nodes=30)
    searches += _read_nearby_find_parents([sha256], ctx.man.binary2.w.alias, max_nodes=30)
    resp = ctx.man.binary2.w.msearch(ctx.sd, searches=searches)
    unique_links, parents, children, cousins = _read_nearby_process_resp(
        resp, in_parents=[sha256], in_children=[sha256]
    )
    logger.debug(f"nearby initial - next query for {len(parents)=} {len(children)=}")

    # Recursively search for all the parents parents and all the children's children.
    seen_parents = set(sha256)
    seen_children = set(sha256)
    seen_cousins = set()
    current_iterations = 0
    # max links to gather per iteration (x2 parent/child)
    max_nodes_per_iteration = 30

    while current_iterations < 10 and (len(parents) > 0 or len(children) > 0 or len(cousins) > 0):
        current_iterations += 1
        # after x links, aggressively filter more connections
        if len(unique_links) > 200:
            max_nodes_per_iteration = 2
        elif len(unique_links) > 500:
            # prevent exponential growth
            max_nodes_per_iteration = 1
        searches = _read_nearby_find_children(children, ctx.man.binary2.w.alias, max_nodes=max_nodes_per_iteration)
        searches += _read_nearby_find_parents(parents, ctx.man.binary2.w.alias, max_nodes=max_nodes_per_iteration)
        # Search for children's parents, parents children, at various depths.
        is_check_for_cousins = (
            include_cousins and len(seen_cousins) < max_cousins and current_iterations <= max_cousin_distance
        )
        # No searches loop has only continued because the len(cousins) > 0.
        # Because is_check_for_cousins is false it's safe to exit
        # Note - This covers the edge case where is_check_for_cousins was true last iteration and false this iteration
        # of the loop. Which can happen due to max_cousins being too large or max_cousin_distance being too large.
        if not is_check_for_cousins and len(searches) == 0:
            break
        if is_check_for_cousins:
            # find parents of children and children of parents (new cousins)
            searches += _read_nearby_find_children(parents, ctx.man.binary2.w.alias, max_nodes=max_nodes_per_iteration)
            searches += _read_nearby_find_parents(children, ctx.man.binary2.w.alias, max_nodes=max_nodes_per_iteration)
            # extend known cousins
            searches += _read_nearby_find_children(cousins, ctx.man.binary2.w.alias, max_nodes=max_nodes_per_iteration)
            searches += _read_nearby_find_parents(cousins, ctx.man.binary2.w.alias, max_nodes=max_nodes_per_iteration)

        resp = ctx.man.binary2.w.msearch(ctx.sd, searches=searches)
        # update seen ids, so we don't query them again
        seen_parents.update(parents)
        seen_children.update(children)
        seen_cousins.update(cousins)
        # parse out new links, and find more parents and children to inspect
        new_unique_links, parents, children, cousins = _read_nearby_process_resp(
            resp, in_parents=parents, in_children=children
        )
        unique_links.update(new_unique_links)
        # filter out parents/children for which we have already gathered their parents/children
        children = children.difference(seen_children)
        parents = parents.difference(seen_parents)
        cousins = cousins.difference(seen_parents).difference(seen_children).difference(seen_cousins)
        logger.debug(
            f"nearby {current_iterations=} {len(unique_links)=} - next query for {len(parents)=} {len(children)=}"
        )

    # Sort for consistency.
    result = list(unique_links.values())

    # sort by child sha256
    # sort by if file format exists - mapped events from external sources may be missing this
    # and in clients such as webui, the last node for a sha256 may be the only one where file metadata is respected
    # sort by source and parent as tiebreakers
    result.sort(
        key=lambda x: (
            x.child,
            True if x.child_node.file_format else False,
            x.source.name if x.source else "",
            x.parent,
        )
    )
    return models_restapi.ReadNearby(id_focus=sha256, links=result)
