"""Delete events from metastore and artifacts from content store.

Public functions should require all arguments to be provided as named arguments to prevent unintended outcomes.

Purge requires use of system context, as it needs to ensure that all connected docs are purged, not just
the portion a user can see.
"""

import copy
import io
import logging
import os
import pathlib
import shutil

import pendulum
from azul_bedrock import models_network as azm
from azul_bedrock.models_restapi import purge as bedr_purge
from opensearchpy import helpers

from azul_metastore import context, settings
from azul_metastore.common import utils
from azul_metastore.query.binary2 import binary_consistency

logger = logging.getLogger(__name__)


class InvalidPurgeException(Exception):
    """The provided arguments to the purge are invalid."""

    pass


class Purger:
    """Create an instance of the data purger."""

    def __init__(self):
        s = settings.get()

        if not s.purge_sha256_folder:
            raise Exception("to purge data, 'purge_sha256_folder' config option must be set")
        # Create purge dir if it does not exist.
        pathlib.Path(s.purge_sha256_folder).mkdir(parents=False, exist_ok=True)
        self._purge_folder = s.purge_sha256_folder

    def _simulate_meta_purge(self, ctx: context.Context, raw_search: dict) -> bedr_purge.PurgeSimulation:
        """Get info about what purge will do."""
        # ensure query is on latest docs
        ctx.refresh()
        # these properties can't be predicted due to need for consistency operations
        # so we just return number of events in the initial delete query
        # FUTURE when this becomes the only ingest method, clean up this model
        events = self._count(ctx, raw_search)
        return bedr_purge.PurgeSimulation(
            events=events,
        )

    def _check_binary_deletion(self, ctx: context.Context, source: str, label: azm.DataLabel, sha256: str):
        """Returns true if there are no documents for the given binary."""
        # retain if there is any reference to the sha256
        raw_search = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "has_child": {
                                "type": "metadata",
                                "query": {
                                    "bool": {
                                        "filter": [
                                            {"term": {"datastreams.sha256": sha256}},
                                            {"term": {"datastreams.label": label}},
                                        ]
                                    }
                                },
                            }
                        },
                        {"has_child": {"type": "metadata", "query": {"term": {"source.name": source}}}},
                    ]
                }
            },
        }
        num_docs = ctx.sd.es().search(index=ctx.man.binary2.w.alias, body=raw_search)["hits"]["total"]["value"]

        return num_docs <= 0

    def _perform_data_deletion(self, ctx: context.Context, source: str, label: azm.DataLabel, sha256: str) -> bool:
        now = pendulum.now()
        _, wasDeleted = ctx.dispatcher.delete_binary(source, label, sha256, now)
        return wasDeleted

    def _delete_event(self, ctx: context.Context, action: azm.DeleteAction, entity: azm.DeleteEvent.DeleteEntity):
        now = pendulum.now(pendulum.UTC).to_iso8601_string()
        author = azm.Author(
            name=ctx.user_info.username,
            category="user",
            security=None,
        )
        return azm.DeleteEvent(
            model_version=azm.CURRENT_MODEL_VERSION,
            kafka_key="tmp",
            author=author,
            timestamp=now,
            entity=entity,
            action=action,
        )

    def _perform_meta_deletion(self, ctx: context.Context, raw_search: dict, ev: azm.DeleteEvent) -> tuple[int, str]:
        """Delete metadata.

        Return number of delete events and filepath to a
        file containing sha256s to be potentially purged from data store.
        """
        path_purge_evaluate = os.path.join(self._purge_folder, f"purge-{ev.timestamp}-evaluate.txt")
        path_purge_binary2 = os.path.join(self._purge_folder, f"purge-{ev.timestamp}-binary2.txt")
        path_purge = os.path.join(self._purge_folder, f"purge-{ev.timestamp}-deleted.txt")
        fcache = set()

        def _update_purge_file(f: io.TextIOBase, source: str, label: azm.DataLabel, sha256: str):
            """Update the sha256 file that will be used for purging binaries."""
            uniq = (source, label, sha256)
            nonlocal fcache
            if uniq in fcache:
                return
            # potential new hash to purge from datastore
            f.write(",".join(uniq) + "\n")
            fcache.add(uniq)
            # stop memory overflows when found lots of hashes
            if len(fcache) > 10000:
                fcache = set()

        # refresh indices before beginning purge
        # important for purges of data that has just been submitted
        ctx.refresh()

        total_purged_count = 0
        for docs in utils.chunker(
            helpers.scan(ctx.sd.es(), index=ctx.man.binary2.w.alias, query=raw_search, size=1000), max_items=1000
        ):
            for doc in docs:
                raw = doc["_source"]
                with open(path_purge_evaluate, "a") as fout:
                    # register all binary data in event, for potential purging
                    for source, label, sha256 in binary_consistency.get_dispatcher_datastreams(ctx, raw["sha256"]):
                        _update_purge_file(fout, source, label, sha256)

            # delete events from metastore
            # this is not a delete_by_query to reduce the chance that new documents are deleted without
            # checking if the data needs to be deleted from dispatcher object storage
            id_deletes = []
            for doc in docs:
                id_deletes.append({"delete": {"_index": doc["_index"], "_id": doc["_id"], "routing": doc["_routing"]}})
            ret = ctx.sd.es().bulk(body=id_deletes)
            if ret["errors"]:
                raise Exception(ret)

            total_purged_count += len(id_deletes)

        if not total_purged_count:
            return 0, None

        # the binary2 ingest needs to figure out parent-child links as they are not stored in every event
        # ensure standard consistency
        total_purged_count += binary_consistency.ensure_valid_binaries(ctx)
        # always check parent binaries
        shutil.copyfile(path_purge_evaluate, path_purge_binary2)

        # check if child binaries should be removed
        with open(path_purge_evaluate, "r") as fin, open(path_purge_binary2, "a") as fout:
            # iterate through all to be deleted for link consistency
            # group sha256s up into chunks
            for sha256s in utils.chunker(x.strip().split(",")[2] for x in fin):
                logger.info(f"removing bad links for {len(sha256s)} binaries")
                # get binaries that are potentially also being removed
                report = binary_consistency.LinkReport()
                for altered_sha256 in binary_consistency.ensure_valid_links(ctx, sha256s, report=report):
                    # retrieve source/datastream info as it is about to be deleted
                    for source, label, sha256 in binary_consistency.get_dispatcher_datastreams(ctx, altered_sha256):
                        _update_purge_file(fout, source, label, sha256)
                total_purged_count += report.total_deleted

        # ensure that children are evaluated too
        path_purge_evaluate = path_purge_binary2

        # ensure that deleted documents are no longer searchable
        ctx.refresh()

        # check if altered binaries should be removed
        with open(path_purge_evaluate, "r") as fin, open(path_purge, "a") as fout:
            # judge which binaries were deleted
            for line in fin:
                source, label, sha256 = line.strip().split(",")
                was_deleted = self._check_binary_deletion(ctx, source, label, sha256)
                logger.info(f"check {sha256} for completely removed {was_deleted=}")
                if was_deleted:
                    fout.write(",".join((source, label, sha256)) + "\n")

        return total_purged_count, path_purge

    def _count(self, ctx: context.Context, search: dict) -> int:
        """Return number of documents matching search."""
        body = copy.deepcopy(search)
        body["track_total_hits"] = True
        # not a .count() due to presence of 'size' and other fields that cause problems with that api
        num_docs = ctx.sd.es().search(index=ctx.man.binary2.w.alias, body=body)["hits"]["total"]["value"]
        return num_docs

    def _purge_path_deletion(self, ctx, filepath: str) -> tuple[int, int]:
        """Purge from a filepath describing sha256s."""
        data_purged = 0
        data_kept = 0
        with open(filepath, "r") as f:
            for line in f:
                source, label, sha256 = line.strip().split(",")
                was_purged = self._perform_data_deletion(ctx, source, label, sha256)
                logger.info(f"{sha256}: {was_purged=}")
                if was_purged:
                    data_purged += 1
                else:
                    data_kept += 1
        return data_kept, data_purged

    def _perform_basic_deletion(
        self, ctx, raw_search: dict, ev: azm.DeleteEvent, *, purge: bool
    ) -> bedr_purge.PurgeResults | bedr_purge.PurgeSimulation:
        """Delete metadata and if skip_data=false, delete data."""
        if not purge:
            return self._simulate_meta_purge(ctx, raw_search)
        # ensure counting is accurate
        ctx.refresh()
        raw_count = self._count(ctx, raw_search)
        if not raw_count:
            raise InvalidPurgeException("nothing to delete")

        ret = bedr_purge.PurgeResults(events_purged=0, binaries_purged=0)

        # delete event to dispatcher
        params = {"name": "metastore-delete", "version": "2021-02-17"}
        ctx.dispatcher.submit_events([ev], model=azm.ModelType.Delete, params=params)
        logger.info("delete event submitted to dispatcher")
        # run up to 9 rounds of deletion (to capture data flowing through from dispatcher)
        for i in range(9):
            # delete metadata over a few rounds to ensure it is complete
            events_purged, purge_path = self._perform_meta_deletion(ctx, raw_search, ev)
            if events_purged == 0:
                break
            ret.events_purged += events_purged

            if not ret.events_purged:
                return ret
            # delete backing binaries if needed
            binaries_kept, binaries_purged = self._purge_path_deletion(ctx, purge_path)
            ret.binaries_kept += binaries_kept
            ret.binaries_purged += binaries_purged
            logger.info(
                f"round {i + 1} of opensearch purging complete, " f"deleted {events_purged} events in this round"
            )
        return ret

    def purge_submission(
        self,
        *,
        track_source_references: str,
        timestamp: str | None,
        purge: bool,
    ) -> bedr_purge.PurgeSimulation | bedr_purge.PurgeResults:
        """Purge binary events associated with a binary submission to a source."""
        ctx = context.get_writer_context()
        # generate delete event for dispatcher
        ev = self._delete_event(
            ctx,
            azm.DeleteAction.submission,
            azm.DeleteEvent.DeleteEntity(
                reason="deleted via metastore api",
                submission=azm.DeleteEvent.DeleteEntity.DeleteSubmission(
                    track_source_references=track_source_references,
                    timestamp=timestamp,
                ),
            ),
        )
        ret = None
        # delete binary2 submission docs
        optional_filters = []
        if timestamp:
            optional_filters.append({"term": {"source.timestamp": timestamp}})
        body = {
            "size": 0,
            "track_total_hits": True,
            "_source": ["sha256"],
            "query": {
                "bool": {
                    "filter": [
                        *optional_filters,
                        {"term": {"track_source_references": track_source_references}},
                    ]
                }
            },
        }
        ret = self._perform_basic_deletion(ctx, body, ev, purge=purge)

        return ret

    def purge_link(
        self,
        *,
        track_link: str,
        purge: bool,
    ) -> bedr_purge.PurgeSimulation | bedr_purge.PurgeResults:
        """Purge manually inserted links between two binaries."""
        ctx = context.get_writer_context()
        # generate delete event for dispatcher
        ev = self._delete_event(
            ctx,
            azm.DeleteAction.link,
            azm.DeleteEvent.DeleteEntity(
                reason="deleted via metastore api",
                link=azm.DeleteEvent.DeleteEntity.DeleteLink(
                    track_link=track_link,
                ),
            ),
        )
        ret = None
        # delete binary2 link docs
        body = {
            "size": 0,
            "track_total_hits": True,
            "_source": ["sha256"],
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"track_link": track_link}},
                    ]
                }
            },
        }
        ret = self._perform_basic_deletion(ctx, body, ev, purge=purge)
        return ret

    def purge_author(
        self,
        *,
        track_author: str,
        purge: bool,
    ) -> bedr_purge.PurgeSimulation | bedr_purge.PurgeResults:
        """Purge binary events associated with a plugin."""
        ctx = context.get_writer_context()
        # Delete all plugins event for the next hour and all historical events.
        delete_event_time = pendulum.now(tz=pendulum.UTC) + pendulum.duration(hours=1)

        # generate delete event for dispatcher
        ev = self._delete_event(
            ctx,
            azm.DeleteAction.author,
            azm.DeleteEvent.DeleteEntity(
                reason="deleted via metastore api",
                author=azm.DeleteEvent.DeleteEntity.DeleteAuthor(
                    track_author=track_author,
                    timestamp=delete_event_time.isoformat().replace("+00:00", "Z"),
                ),
            ),
        )
        ret = None
        # delete binary2 docs
        body = {
            "size": 0,
            "track_total_hits": True,
            "_source": ["sha256"],
            "query": {
                "bool": {
                    "should": [
                        {"term": {"track_author": track_author}},
                        {"term": {"parent_track_author": track_author}},
                    ],
                    "minimum_should_match": 1,
                }
            },
        }
        ret = self._perform_basic_deletion(ctx, body, ev, purge=purge)

        return ret
