"""Routes for purge data queries."""

import pendulum
from azul_bedrock.models_restapi import purge as bedr_purge
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response
from pendulum.parsing.exceptions import ParserError
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_403_FORBIDDEN

from azul_metastore import context
from azul_metastore.query import purge as qpurge
from azul_metastore.restapi.quick import qr

router = APIRouter()


@router.delete(
    "/v0/purge/submission/{track_source_references}",
    response_model=qr.gr(bedr_purge.PurgeSimulation | bedr_purge.PurgeResults),
    **qr.kw,
)
def purge_submission(
    resp: Response,
    track_source_references: str,
    timestamp: str = Query(description="Timestamp of submission to purge."),
    purge: bool = Query(False, description="Perform purge instead of simulation."),
    ctx: context.Context = Depends(qr.ctx),
):
    """Purge a set of submissions."""
    if not ctx.is_admin():
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail=f"user '{ctx.user_info.username}' not superuser",
        )
    try:
        pendulum.parse(timestamp)
    except ParserError:
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST, detail=f"The timestamp provided '{timestamp}' has an invalid format."
        )

    purger = qpurge.Purger()
    try:
        ret = purger.purge_submission(
            track_source_references=track_source_references,
            timestamp=timestamp,
            purge=purge,
        )
    except qpurge.InvalidPurgeException as e:
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    return qr.fr(ctx, ret or {}, resp)


@router.delete(
    "/v0/purge/link/{track_link}",
    response_model=qr.gr(bedr_purge.PurgeSimulation | bedr_purge.PurgeResults),
    **qr.kw,
)
def purge_link(
    resp: Response,
    track_link: str = Path(..., description="Tracking information of link to remove."),
    purge: bool = Query(False, description="Perform purge instead of simulation."),
    ctx: context.Context = Depends(qr.ctx),
):
    """Purge a manually added relationship between binaries."""
    if not ctx.is_admin():
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail=f"user '{ctx.user_info.username}' not superuser",
        )

    purger = qpurge.Purger()
    try:
        ret = purger.purge_link(
            track_link=track_link,
            purge=purge,
        )
    except qpurge.InvalidPurgeException as e:
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    return qr.fr(ctx, ret or {}, resp)
