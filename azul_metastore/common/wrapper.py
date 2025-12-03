"""Wrapper info around opensearch class access."""

import copy
import json
import logging
import time
import types
from typing import Any, Iterable

import opensearchpy
from azul_bedrock.models_restapi.basic import QueryInfo
from opensearchpy import helpers

from azul_metastore.common import search_data, utils
from azul_metastore.common.query_info import IngestError

logger = logging.getLogger(__name__)

# 'constants' that are modified during tests.
# For a delete_loop, the max number of docs to delete each loop
MAX_DOCS_DELETED_PER_QUERY = 10000
# For a delete_loop, if no more than this many docs are deleted, finish
MIN_DOCS_DELETED_PER_QUERY = 100


class InvalidSearchException(Exception):
    """Arguments to search were invalid."""

    pass


class IndexException(Exception):
    """Exception when attempting to index the documents."""

    pass


def partition_key_to_indices(partition: str, key: str) -> list[str]:
    """Partition and key joined to reference set of indices."""
    return [
        f"azul.x.{partition}.{key}.*",
        f"azul.o.{partition}.{key}.*",
        f"azul.x.{partition}.{key}",
        f"azul.o.{partition}.{key}",
    ]


def set_index_properties(partition: str, key: str, data: dict) -> None:
    """Set properties for an index pattern."""
    body = {
        "index_patterns": partition_key_to_indices(partition, key),
        "order": len(key) + 10,
        "settings": data,
    }
    sd = search_data.get_writer_search_data()
    sd.es().indices.put_template(name=f"azul.{partition}.{key}prop", body=body)


class InitFailure(Exception):
    """Opensearch indices could not be verified/initialised for metastore."""

    pass


class TimeAndLogCommand:
    """Log an Opensearch commands run and time the command."""

    class Proxy:
        """Proxy any requests to the OpenSearch instance so we can record responses."""

        def __init__(self, es: opensearchpy.OpenSearch) -> None:
            self._es = es
            self.last_resp = None
            self.last_name = ""

        def _callback(self, resp: Any):
            self.last_resp = resp
            return resp

        def __getattr__(self, __name: str) -> Any:
            """Wrap OpenSearch methods to track responses."""
            attr = self._es.__getattribute__(__name)
            if type(attr) is types.MethodType:
                self.last_name = __name
                return lambda *args, **kwargs: self._callback(attr(*args, **kwargs))
            return attr

    def __init__(
        self, sd: search_data.SearchData, index: str, body: dict | list[dict], query_type: str, *args, **kwargs
    ):
        self._enable_log_es_queries = sd.enable_log_es_queries
        if self._enable_log_es_queries:
            # only format the json string if we need to log it (slow)
            # json is desired as it can be pasted into opensearch dashboard dev tools
            try:
                encoded = json.dumps(body)
            except Exception as e:
                encoded = repr(e)
            logger.debug(
                "query: query_type='%s' index='%s' args='%s' kwargs='%s' body as follows\n%s",
                query_type,
                index,
                args,
                kwargs,
                encoded,
            )

        self._es = self.Proxy(sd.es())
        self._enable_capture_es_queries = sd.enable_capture_es_queries

        self.query_info = None
        # Log the data for the query
        if self._enable_capture_es_queries:
            args = None if len(args) == 0 else list(args)
            if len(kwargs) == 0:
                kwargs = None
            self.query_info = QueryInfo(query_type=query_type, index=index, query=body, args=args, kwargs=kwargs)
            sd.captured_es_queries.append(self.query_info)

    def __enter__(self) -> opensearchpy.OpenSearch:
        """Start timing the functions run.

        Pretend to return opensearchpy.OpenSearch so autocompletion still works.
        """
        self.start_time = time.time()
        return self._es

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Save the total time the function was running for."""
        if self.query_info:
            self.query_info.run_time_ms = int((time.time() - self.start_time) * 1000)

        if self._enable_log_es_queries:
            # only format the json string if we need to log it (slow)
            try:
                encoded = json.dumps(self._es.last_resp)
            except Exception as e:
                encoded = repr(e)
                # only print limited amout of json response as may be very big
            logger.debug("%s resp %s", self._es.last_name, encoded[:10000])
        if self._enable_capture_es_queries:
            self.query_info.response = self._es.last_resp


class Wrapper:
    """A wrapper object for accessing opensearch."""

    def __init__(
        self,
        partition: str,
        docname: str,
        index_settings: dict,
        minimum_required_access: frozenset[str],
        mapping: dict,
        version: int,
    ):
        """Handle direct interaction with opensearch."""
        self.index_settings = index_settings
        self.mapping = mapping
        self.docname = docname
        self.version = version

        self.index_open = f"azul.o.{partition}.{self.docname}"
        self.index_shut = f"azul.x.{partition}.{self.docname}"
        self.alias = f"azul.{partition}.{self.docname}"

        self.minimum_required_access = minimum_required_access

    def initialise(self, sd: search_data.SearchData, *, force: bool = False):
        """Create the template, alias and default index if required."""
        # create initial index for data
        try:
            existing_template = sd.es().indices.get_template(name=self.alias, ignore=[404]).get(self.alias, {})
        except opensearchpy.AuthenticationException as e:
            raise InitFailure(f"{self.alias} {e.error}")

        # Check if the version is absent or the configured number of shards and replicas has changed and
        # update index template if required.
        if (
            "version" not in existing_template
            or existing_template.get("settings", {}).get("index", {}).get("number_of_shards", -1)
            != self.index_settings.get("number_of_shards", 3)
            or existing_template.get("settings", {}).get("index", {}).get("number_of_replicas", -1)
            != self.index_settings.get("number_of_replicas", 2)
            or force
        ):
            # template does not exist or must be replaced, and needs to be updated
            self.update_mapping(sd, mapping=self.mapping)
            try:
                sd.es().indices.create(index=self.index_open, ignore=400)
            except Exception as e:
                raise InitFailure(f"Failure creating {self.index_open} index") from e
            logger.info(f"{self.alias} template updated or created")
        elif existing_template["version"] != self.version:
            # do not replace automatically unless forced
            # this indicates that data needs to be reindexed
            raise InitFailure(
                f'{self.alias} template ({existing_template["version"]}) does not match metastore ({self.version}). '
                "Consider using a new metastore partition and reindexing data."
            )
        else:
            logger.info(f"{self.alias} template ok")

        # create open index if it doesn't exist
        # required for aliases to work as expected
        if not sd.es().indices.exists(index=self.index_open):
            try:
                sd.es().indices.create(index=self.index_open)
            except Exception as e:
                raise InitFailure(f"Failure creating {self.index_open} index") from e
            logger.info(f"{self.alias} alias {self.index_open} index created")
        else:
            logger.info(f"{self.alias} alias {self.index_open} index ok")

    def get_subalias(self, sub: str):
        """Return an alias for a sub resource under the primary event type.

        e.g. azul.binary.mysource01 searches all open and closed indices for the source.
        """
        return f"{self.alias}.{sub}"

    def create_or_update_subalias(self, sd: search_data.SearchData, sub: str):
        """Create an alias for a sub resource under the primary event type.

        e.g. azul.binary.mysource01 searches all open and closed indices for the source.
        """
        alias = self.get_subalias(sub)
        template = {
            "aliases": {alias: {}},
            "index_patterns": [f"{self.index_open}.{sub}.*", f"{self.index_shut}.{sub}.*"],
            "order": 10,
        }

        # Check if the template exists and that it's index pattern hasn't changed.
        if sd.es().indices.exists_template(name=alias):
            existing_template = sd.es().indices.get_template(name=alias)
            etv = existing_template.get(alias)
            if (
                etv.get("aliases") == template.get("aliases")
                and etv.get("index_patterns") == template.get("index_patterns")
                and etv.get("order") == template.get("order")
            ):
                logger.info(f"{alias} template already exists in opensearch and is not being updated.")
                return

        # don't care if it already exists
        sd.es().indices.put_template(name=alias, body=template)
        logger.info(f"{alias=} created in opensearch")

    def get_template(self, sd: search_data.SearchData):
        """Return the template from opensearch."""
        return sd.es().indices.get_template(name=self.alias)[self.alias]

    def update_mapping(self, sd: search_data.SearchData, mapping: dict):
        """Update mappings with new properties (does not rename or delete, will fail instead)."""
        template = {
            "settings": self.index_settings,
            "aliases": {self.alias: {}},
            "mappings": mapping,
            "index_patterns": [self.index_open, self.index_open + ".*", self.index_shut, self.index_shut + ".*"],
            "order": 1,
            "version": self.version,
        }
        # update template first, so existing indices can be manually fixed/rebuilt off of template if needed
        sd.es().indices.put_template(name=self.alias, body=template)
        sd.es().indices.put_mapping(body=template["mappings"], index=self.index_open, ignore=404)
        sd.es().indices.put_mapping(body=template["mappings"], index=self.index_open + ".*", ignore=404)
        sd.es().indices.put_mapping(body=template["mappings"], index=self.index_shut, ignore=404)
        sd.es().indices.put_mapping(body=template["mappings"], index=self.index_shut + ".*", ignore=404)

    def get_indices(self, sd: search_data.SearchData, **kwargs) -> dict:
        """Return all indices for the encoder."""
        return sd.es().indices.get(index=self.alias, **kwargs)

    def _limit_search_complex(self, sd: search_data.SearchData, body: dict) -> dict:
        # make a copy as we are modifying the original query
        body = copy.deepcopy(body)
        query = body.setdefault("query", {})
        # Inject the security limiter into kNN operators if available (as it has a inner option for filtering
        # which is faster and avoids issues around no results being returned as kNN has a finite limit)
        if set(query.keys()) == {"knn"}:
            knn_filter = query["knn"]
            if len(knn_filter) != 1:
                # This gets more complicated with security filters; don't worry about this edge case
                raise Exception("kNN filters only supported for one search term")
            query = knn_filter[list(knn_filter.keys())[0]].setdefault("filter", {})

        tmp = query.setdefault("bool", {})
        if set(query.keys()) != {"bool"}:
            raise Exception("Can only have bool in top level query (or within kNN query filter)")

        tmp.setdefault("must_not", [])
        tmp.setdefault("filter", [])
        has_child = False
        for f in body["query"]["bool"]["filter"]:
            if "has_child" in f and "query" in f["has_child"]:
                has_child = True
                break

        if sd.security_exclude:
            # convert to safe format
            safes = utils.azsec().unsafe_to_safe(sd.security_exclude)
            if not sd.security_include:

                tmp["must_not"] += [
                    {"terms": {"encoded_security.inclusive": safes}},
                    {"terms": {"encoded_security.exclusive": safes}},
                    {"terms": {"encoded_security.markings": safes}},
                ]
            else:
                # add must not clause to children
                must_not_clause = []
                for value in safes:
                    if "-rel-" in value:
                        must_not_clause.append({"term": {"encoded_security.inclusive": value}})

                for f in body["query"]["bool"]["filter"]:
                    if "has_child" in f and "query" in f["has_child"]:
                        hc_query = f["has_child"]["query"]

                        # Wrap non-bool query
                        if "bool" not in hc_query:
                            f["has_child"]["query"] = {"bool": {"must_not": must_not_clause}}
                    # onlly add to one has_child in the query
                    break

        if sd.security_include:  # user has specified AND search based on RELs
            if has_child:
                # Convert to safe format and build AND-style term clauses
                musts = utils.azsec().unsafe_to_safe(sd.security_include)
                must_clauses = [{"term": {"encoded_security.inclusive": value}} for value in musts]

                for f in body["query"]["bool"]["filter"]:
                    if "has_child" in f and "query" in f["has_child"]:
                        hc_query = f["has_child"]["query"]

                        # Wrap non-bool query
                        if "bool" not in hc_query:
                            f["has_child"]["query"] = {"bool": {"must": [hc_query] + must_clauses}}
                        else:
                            # Replace any 'terms' clause targeting encoded_security.inclusive
                            existing_must = hc_query["bool"].get("must", [])
                            new_must = []

                            for clause in existing_must:
                                if (
                                    "terms" in clause
                                    and isinstance(clause["terms"], dict)
                                    and "encoded_security.inclusive" in clause["terms"]
                                ):
                                    continue  # skip the old terms clause
                                new_must.append(clause)

                            # Add individual term clauses (AND logic)
                            new_must.extend(must_clauses)
                            hc_query["bool"]["must"] = new_must
                        # only add to one has_child in the query
                        break

        return body

    def _limit_search(self, sd: search_data.SearchData, body: dict) -> dict:
        # make a copy as we are modifying the original query
        body = copy.deepcopy(body)
        query = body.setdefault("query", {})
        # Inject the security limiter into kNN operators if available (as it has a inner option for filtering
        # which is faster and avoids issues around no results being returned as kNN has a finite limit)
        if set(query.keys()) == {"knn"}:
            knn_filter = query["knn"]
            if len(knn_filter) != 1:
                # This gets more complicated with security filters; don't worry about this edge case
                raise Exception("kNN filters only supported for one search term")
            query = knn_filter[list(knn_filter.keys())[0]].setdefault("filter", {})

        tmp = query.setdefault("bool", {})
        if set(query.keys()) != {"bool"}:
            raise Exception("Can only have bool in top level query (or within kNN query filter)")

        tmp.setdefault("must_not", [])
        tmp.setdefault("must", [])
        if not isinstance(tmp["must"], list):
            tmp["must"] = [tmp["must"]]
        tmp.setdefault("filter", [])

        if sd.security_exclude and not sd.security_include:
            # convert to safe format
            safes = utils.azsec().unsafe_to_safe(sd.security_exclude)
            tmp["must_not"] += [
                {"terms": {"encoded_security.inclusive": safes}},
                {"terms": {"encoded_security.exclusive": safes}},
                {"terms": {"encoded_security.markings": safes}},
            ]
        elif sd.security_exclude:
            # convert to safe format
            safes = utils.azsec().unsafe_to_safe(sd.security_exclude)
            tmp["must_not"] += [
                {"terms": {"encoded_security.exclusive": safes}},
                {"terms": {"encoded_security.markings": safes}},
            ]

            for value in safes:
                if "-rel-" in value:
                    tmp["must_not"].append({"term": {"encoded_security.inclusive": value}})

        if sd.security_include:  # user has specified AND search based on RELs
            # Convert to safe format and build AND-style term clauses
            musts = utils.azsec().unsafe_to_safe(sd.security_include)
            for m in musts:
                tmp["must"].append({"term": {"encoded_security.inclusive": m}})

        return body

    def count(self, sd: search_data.SearchData, body: dict, *args, **kwargs):
        """Perform basic opensearch count."""
        body = self._limit_search(sd, body)
        with TimeAndLogCommand(sd, self.alias, body, "count", **kwargs) as es:
            return es.count(index=self.alias, body=body, **kwargs)

    def delete(self, sd: search_data.SearchData, body: dict, **kwargs):
        """Perform basic opensearch delete by query."""
        body = self._limit_search(sd, body)
        with TimeAndLogCommand(sd, self.alias, body, "delete_by_query", **kwargs) as es:
            return es.delete_by_query(index=self.alias, body=body, ignore=[404], conflicts="proceed", **kwargs)

    def delete_loop(self, sd: search_data.SearchData, body: dict, *, subalias: str = "") -> int:
        """Perform looping opensearch delete by query.

        Returns number of documents deleted.
        """
        alias = self.alias
        if subalias:
            alias = self.get_subalias(subalias)
        docs_removed = 0
        while True:
            # Delete by query and ignore conflicts, conflicts occur when already deleted documents appear in the query.
            # wrapper is not used here as we need to ensure a specific alias is targeted (usually per source).
            resp = sd.es().delete_by_query(
                index=alias,
                body=body,
                ignore=[404],
                conflicts="proceed",
                max_docs=MAX_DOCS_DELETED_PER_QUERY,
                scroll_size=MAX_DOCS_DELETED_PER_QUERY,
            )
            docs_removed_in_iteration = resp.get("deleted", 0)
            docs_removed += docs_removed_in_iteration
            # version conflicts reduce total deleted, so we use a smaller number here
            if docs_removed_in_iteration < MIN_DOCS_DELETED_PER_QUERY:
                break
            logger.info(
                f"deleted: {docs_removed_in_iteration} documents (total {docs_removed}) "
                f"from {alias=} and still more to delete."
            )
        if docs_removed > 0:
            logger.info(f"deleted: a total of {docs_removed} documents from {alias=} completed")

        # Handle errors
        if resp.get("error", {}).get("type") == "index_not_found_exception":
            logger.warning(f"{alias=} no indices")
            return docs_removed
        if resp.get("error") is not None:
            logger.warning(f"{alias=} failed to delete documents with error {resp["error"]}")
            return docs_removed

        # Process response metadata if no errors.
        return docs_removed

    def update(self, sd: search_data.SearchData, body: dict, **kwargs):
        """Update an existing document in opensearch."""
        body = self._limit_search(sd, body)
        with TimeAndLogCommand(sd, self.alias, body, "update_by_query", **kwargs, conflicts="proceed") as es:
            return es.update_by_query(index=self.alias, body=body, conflicts="proceed", **kwargs)

    def get(self, sd: search_data.SearchData, _id: str):
        """Retrieve documents by id over simple indices.

        Won't work with custom categoriser due to targeting specific indices.
        """
        with TimeAndLogCommand(sd, self.index_open, {}, "get", id=_id) as es:
            resp1 = es.get(index=self.index_open, id=_id, ignore=404)

        ret = resp1["_source"] if resp1.get("found") else None
        if not ret:
            with TimeAndLogCommand(sd, self.index_shut, {}, "get", id=_id) as es:
                resp2 = es.get(index=self.index_shut, id=_id, ignore=404)
            ret = resp2["_source"] if resp2.get("found") else ret
        return ret

    def complex_search(self, sd: search_data.SearchData, body: dict, **kwargs):
        """Presence of children with security excludes/includes make this more complex."""
        body = self._limit_search_complex(sd, body)
        with TimeAndLogCommand(sd, self.alias, body, "search", **kwargs) as es:
            return es.search(index=self.alias, body=body, **kwargs)

    def search(self, sd: search_data.SearchData, body: dict, **kwargs):
        """Perform basic opensearch query."""
        body = self._limit_search(sd, body)
        with TimeAndLogCommand(sd, self.alias, body, "search", **kwargs) as es:
            return es.search(index=self.alias, body=body, **kwargs)

    def msearch(self, sd: search_data.SearchData, searches: list[dict], **kwargs):
        """Perform multiple basic opensearch query."""
        for i, body in enumerate(searches):
            if i % 2 == 0:
                # only process search bodies
                continue
            body = self._limit_search(sd, body)

        with TimeAndLogCommand(sd, self.alias, searches, "msearch", **kwargs) as es:
            return es.msearch(index=self.alias, body=searches, max_concurrent_searches=1, **kwargs)

    def scan(self, sd: search_data.SearchData, body: dict, **kwargs):
        """Return a scan helper for retrieving all matching documents."""
        body = self._limit_search(sd, body)
        with TimeAndLogCommand(sd, self.alias, body, "scan", **kwargs) as es:
            return helpers.scan(es, index=self.alias, query=body, **kwargs)

    def wrap_docs(self, raw_docs: Iterable[dict]):
        """Wrap all docs for bulk opensearch creation."""
        rows = []
        for doc in raw_docs:
            if doc.pop("_binary_index", False):
                # update is only used by binary indexer
                # always goes into secure zone (expecting parent-child relationship)
                index = self.index_shut
            else:

                # make decision if goes to dls or non-dls index
                check_open = frozenset(
                    doc["encoded_security"].get("exclusive", [])
                    + doc["encoded_security"].get("inclusive", [])
                    + utils.azsec().get_enforceable_markings(doc["encoded_security"].get("markings", []))
                )
                index = self.index_open if check_open.issubset(self.minimum_required_access) else self.index_shut

            # perform doc categorisation
            index += doc.pop("_index_extension", "")
            op_type = doc.pop("_op_type", "index")
            tmp = {
                "_op_type": op_type,
                "_index": index,
                "_id": doc.pop("_id"),
                "_source": doc,
            }
            if "_routing" in doc:
                tmp["_routing"] = doc.pop("_routing")
            rows.append(tmp)

        return rows

    @classmethod
    def _map_errors_to_wrapped(cls, rows: list[dict], errors: dict) -> list[IngestError]:
        """Maps errors from opensearch generated when trying to index a document to the original document.

        Assumes no duplicate ids for raw_results.
        """
        bad_raw_results = []
        if errors:
            results_mapping = dict()
            # Match documents to one another using their Ids
            for cur_result in rows:
                # read using expected opensearch id
                results_mapping[cur_result["_id"]] = cur_result
            for doc in errors:
                if "create" in doc:
                    internal = doc["create"]
                    if doc["create"]["error"]["type"] == "version_conflict_engine_exception":
                        # not an error we care about, as we only create the doc if not existing
                        continue
                elif "index" in doc:
                    internal = doc["index"]
                elif "update" in doc:
                    internal = doc["update"]
                else:
                    raise Exception(f"unknown doc type to handle error in {doc}")

                try:
                    etype = internal["error"]["type"]
                except KeyError:
                    etype = "no-type"
                try:
                    ereason = internal["error"]["reason"]
                except KeyError:
                    ereason = "no-reason"

                # get input doc (after id/routing pop)
                source = results_mapping[internal["_id"]].get("_source")
                if not source:
                    source = results_mapping[internal["_id"]]["upsert"]
                bad_raw_results.append(
                    IngestError(
                        doc=source,
                        error_type=etype,
                        error_reason=ereason,
                    )
                )
        return bad_raw_results

    @classmethod
    def index_docs(
        cls, sd: search_data.SearchData, docs: list[dict], refresh: bool = False, raise_on_errors: bool = False
    ) -> list[IngestError]:
        """Save supplied documents to opensearch.

        :param sd: SearchData used to get the opensearch client.
        :param docs: list of docs to save
        :param refresh: Whether or not to refresh the index after the documents are added to ensure they are ready to
                        be read.
        :param raise_on_errors: If True, Errors during indexing are raised as exception stopping processing.
                                If False, a list of errors that occurred are returned instead.

        :returns: list of errors that occurred if (raise_on_errors if False) otherwise an empty list.
        """
        if not docs:
            return []

        # write docs to opensearch and update index
        errors = []
        if not refresh and len(docs) > 1:
            # complete in parallel since we aren't waiting for the data to be searchable
            for success, err in helpers.parallel_bulk(
                sd.es(), docs, request_timeout=200, refresh=refresh, raise_on_error=raise_on_errors
            ):
                if not success:
                    errors.append(err)
        else:
            # wait for all data to be searchable
            success_count_with_errors = helpers.bulk(
                sd.es(), docs, request_timeout=200, refresh=refresh, raise_on_error=raise_on_errors
            )
            errors = success_count_with_errors[1]

        # map errors to original docs
        return cls._map_errors_to_wrapped(docs, errors)

    def wrap_and_index_docs(
        self, sd: search_data.SearchData, docs: list[dict], refresh: bool = False, raise_on_errors: bool = True
    ) -> list[IngestError]:
        """Save supplied documents to opensearch.

        :param sd: SearchData used to get the opensearch client.
        :param docs: list of docs to save
        :param refresh: Whether or not to refresh the index after the documents are added to ensure they are ready to
                        be read.
        :param raise_on_errors: If True, Errors during indexing are raised as exception stopping processing.
                                If False, a list of errors that occurred are returned instead.

        :returns: list of errors that occurred if (raise_on_errors if False) otherwise an empty list.
        """
        # wrap to set index and other metadata
        rows = self.wrap_docs(docs)
        return self.index_docs(sd, rows, refresh, raise_on_errors)
