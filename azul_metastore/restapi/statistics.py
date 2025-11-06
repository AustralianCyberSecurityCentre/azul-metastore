"""General reporting endpoints.

Statistics are generated periodically, with a process-local and OpenSearch cache to minimise requests made.

Current statistics:
- Total number of binaries (ignoring security)

Statistics take a while to generate, so we need to avoid making too many requests to OpenSearch to avoid DOSing
it. When statistics in both the process-local and OpenSearch cache are deemed stale, a single task will be run
on a per-process level to update these statistics. A global lock is not feasible, but process-local locks should
at least reduce the impact from racing to update statistics.
"""

import logging
import threading
import time
from typing import Optional

from azul_bedrock.models_restapi import statistics as bedr_statistics
from fastapi import APIRouter, BackgroundTasks, Depends, Response
from pydantic import ValidationError

from azul_metastore import context
from azul_metastore.query import cache
from azul_metastore.query.binary2 import binary_read
from azul_metastore.restapi.quick import qr

logger = logging.getLogger(__name__)

router = APIRouter()


# Version of StatisticSummary. Should be incremented when that model is changed.
MODEL_VERSION = "v1"

# Time, in seconds, of how often statistics should be updated.
UPDATE_INTERVAL = 60 * 30  # 30 minutes

# Determine if an update for statistics is in progress.
_currently_updating = False

# Global (process-local) lock for making changes to _latest_statistics.
# We need a global lock to avoid having n many local threads try to hit OpenSearch with 30+ second requests
# - it can handle it, but given that statistics are shared it is purely a waste of electricity.
_updating_mutex = threading.Lock()


def _stats_need_update(stats: bedr_statistics.StatisticContainer) -> bool:
    """Determines if cached statistics are stale."""
    return stats.timestamp + UPDATE_INTERVAL < time.time() and not _currently_updating


def _read_cached_statistics() -> Optional[bedr_statistics.StatisticContainer]:
    """Reads the latest statistics object from OpenSearch, if available."""
    # Use the admin user to fetch all stats regardless of who initiated the request
    ctx = qr.writer

    statistics_obj = cache.load_generic(ctx, "statistics", "global", MODEL_VERSION)

    if statistics_obj is not None:
        try:
            return bedr_statistics.StatisticContainer.model_validate(statistics_obj)
        except ValidationError as e:
            logger.error("Failed to validate cached statistics: %s", str(e))

    return None


def _update_statistics() -> bedr_statistics.StatisticContainer:
    """Updates statistics with the system context."""
    global _currently_updating

    logger.info("Updating global statistics...")

    try:
        # Use the admin user to fetch all stats regardless of who initiated the request
        ctx = qr.writer

        # Build new results
        count = binary_read.get_total_binary_count(ctx)

        new_container = bedr_statistics.StatisticContainer(
            timestamp=int(time.time()), data=bedr_statistics.StatisticSummary(binary_count=count)
        )

        logger.info("Global statistics updated.")

        # Update the global cache
        cache.store_generic(
            ctx,
            "statistics",
            "global",
            MODEL_VERSION,
            new_container.model_dump(mode="json"),
        )

        return new_container
    finally:
        _currently_updating = False


@router.get("/v0/statistics", response_model=qr.gr(bedr_statistics.StatisticSummary), **qr.kw)
def get_statistics(resp: Response, background_tasks: BackgroundTasks, ctx: context.Context = Depends(qr.ctx)):
    """Read a summary of various global attributes about this instance of Azul."""
    global _currently_updating

    # Read in the cached object from OpenSearch, if one if available
    statistics_container = _read_cached_statistics()

    if statistics_container is None or _stats_need_update(statistics_container):
        # No statistics available and/or they are stale; we might need to update these
        # Risk entering the mutex now:
        with _updating_mutex:
            # Avoid a race condition and check again now that we know we are the only thread
            # (in this process) who can make changes tn ensure that they haven't been updated
            # elsewhere
            statistics_container = _read_cached_statistics()

            if statistics_container is None:
                # Still no statistics available; update them in a blocking fashion
                _currently_updating = True
                logger.info("Fetching new statistics immediately - no cache available.")
                statistics_container = _update_statistics()
            elif _stats_need_update(statistics_container):
                # We have some old cached stats - we can procrastinate for a bit
                _currently_updating = True
                logger.info("Updating stale statistics in background...")
                background_tasks.add_task(_update_statistics)

        # The mutex will be exited at this point, but as we have set currently_updating,
        # no other thread will enter the mutex until a result has been obtained

    return qr.fr(ctx, statistics_container.data, resp)
