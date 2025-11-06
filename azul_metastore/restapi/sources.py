"""Routes for source data queries."""

from datetime import datetime

from azul_bedrock import models_settings
from azul_bedrock.models_restapi import sources as bedr_sources
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from starlette.status import HTTP_404_NOT_FOUND

from azul_metastore import context, settings
from azul_metastore.query.binary2 import binary_source as binary_source
from azul_metastore.restapi.quick import qr

router = APIRouter()


@router.get("/v0/sources", response_model=qr.gr(dict[str, models_settings.Source]), **qr.kw)
def get_all_sources(
    resp: Response,
    ctx: context.Context = Depends(qr.ctx_without_queries),
):
    """Read summary info for all sources."""
    data = binary_source.read_sources()
    if not data:
        qr.set_security_headers(ctx, resp)
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)
    return qr.fr(ctx, data, resp)


@router.head("/v0/sources/{source}", **qr.kw)
def check_source_exists(
    resp: Response,
    source: str,
    ctx: context.Context = Depends(qr.ctx_without_queries),
):
    """Read basic source information."""
    try:
        if not settings.check_source_exists(source):
            raise HTTPException(status_code=HTTP_404_NOT_FOUND)
    finally:
        qr.set_security_headers(ctx, resp)


@router.get("/v0/sources/{name}", response_model=qr.gr(bedr_sources.Source), **qr.kw)
def read_source(
    resp: Response,
    name: str,
    ctx: context.Context = Depends(qr.ctx),
):
    """Read basic source information."""
    if not settings.check_source_exists(name):
        qr.set_security_headers(ctx, resp)
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)
    data = binary_source.read_source(ctx, name)
    return qr.fr(ctx, data, resp)


@router.get("/v0/sources/{source}/references", response_model=qr.gr(bedr_sources.References), **qr.kw)
def source_refs_read(
    resp: Response,
    source: str,
    ctx: context.Context = Depends(qr.ctx),
    term: str = Query(""),
):
    """Read source reference details."""
    rows = binary_source.read_source_references(ctx, source, term=term)
    if not rows:
        qr.set_security_headers(ctx, resp)
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)
    return qr.fr(ctx, {"items": rows}, resp)


@router.get("/v0/sources/{source}/submissions", response_model=qr.gr(bedr_sources.References), **qr.kw)
def source_submissions_read(
    resp: Response,
    source: str,
    ctx: context.Context = Depends(qr.ctx),
    track_source_references: str = Query(
        None, description="Tracking submission ID for the submission that is being searched for."
    ),
    timestamp: datetime = Query(None, description="Timestamp of submission when you are looking for a specific one."),
):
    """List all sources or find a specific source."""
    rows = binary_source.read_submissions(
        ctx, source, track_source_references=track_source_references, submission_timestamp=timestamp
    )
    if not rows:
        qr.set_security_headers(ctx, resp)
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)
    return qr.fr(ctx, {"items": rows}, resp)
