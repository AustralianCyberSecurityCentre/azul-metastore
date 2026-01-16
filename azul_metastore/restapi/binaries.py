"""Routes for entity data queries."""

from typing import Optional
from urllib.parse import unquote

import pendulum
from azul_bedrock import models_network as azm
from azul_bedrock.exceptions import ApiException
from azul_bedrock.models_restapi import binaries as bedr_binaries
from azul_bedrock.models_restapi.binaries_auto_complete import AutocompleteContext
from azul_security.exceptions import SecurityAccessException, SecurityParseException
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    HTTPException,
    Query,
    Response,
)
from starlette.status import HTTP_404_NOT_FOUND, HTTP_422_UNPROCESSABLE_CONTENT

from azul_metastore import context
from azul_metastore.common import wrapper
from azul_metastore.common.search_query import validate_term_query
from azul_metastore.encoders.annotation import InvalidAnnotation
from azul_metastore.query import annotation, status
from azul_metastore.query.binary2 import (
    binary_event,
    binary_find,
    binary_find_paginate,
    binary_read,
    binary_related,
    binary_similar,
    binary_summary,
)
from azul_metastore.restapi.quick import qr

router = APIRouter()


@router.get("/v0/binaries/tags", response_model=qr.gr(bedr_binaries.ReadTags), **qr.kw)
def get_all_tags_on_binaries(resp: Response, ctx: context.Context = Depends(qr.ctx)):
    """Read existing tags across all entities."""
    return qr.fr(ctx, annotation.read_all_binary_tags(ctx), resp)


@router.get("/v0/binaries", response_model=qr.gr(bedr_binaries.EntityFind), **qr.kw)
@router.post("/v0/binaries", response_model=qr.gr(bedr_binaries.EntityFind), **qr.kw)
def find_binaries(
    resp: Response,
    hashes: list[str] = Body([], embed=True),
    term: str = Query(None, description="A free text Azul search term"),
    sort: bedr_binaries.FindBinariesSortEnum = Query(
        bedr_binaries.FindBinariesSortEnum.score,
        description="Sort results by this property.",
    ),
    sort_asc: bool = Query(
        False,
        description="Sort results in ascending order.",
    ),
    max_entities: int = Query(100, description="Maximum number of binaries to return"),
    count_entities: bool = Query(False, description="Also return the total number of binaries that match the search"),
    ctx: context.Context = Depends(qr.ctx),
):
    """Find entities in the system based on search term.

    If hashes specified in body, rows will be returned in same order as hashes.
    Missing hashes will still return a row. Duplicate hashes will have after-first-entry omitted.
    """
    print("THIS IS CTX: ", ctx)
    try:
        data = binary_find.find_binaries(
            ctx,
            term=term,
            sort=sort.value,
            sort_asc=sort_asc,
            max_binaries=max_entities,
            count_binaries=count_entities,
            hashes=hashes,
        )
    except wrapper.InvalidSearchException as e:
        raise HTTPException(status_code=400, detail=e.args[0]) from e

    # Check if term query was valid if no data was found from the request.
    if data.items_count == 0 and term:
        # Check if grammar was appropriate
        model_valid_keys = binary_event.get_opensearch_binary_mapping(qr.writer)
        invalid_keys = validate_term_query(term, model_valid_keys)
        if len(invalid_keys) > 0:
            opt_s = "s" if len(invalid_keys) > 1 else ""
            raise HTTPException(
                status_code=422,
                detail=f"The following term query key{opt_s} could not be found: [{', '.join(invalid_keys)}]. "
                + f"Either the key{opt_s} are related to results and temporarily missing or the query is invalid.",
            )
    print("Responce ", qr.fr(ctx, data, resp))
    return qr.fr(ctx, data, resp)


@router.post("/v0/binaries/all", response_model=qr.gr(bedr_binaries.EntityFindSimple), **qr.kw)
def find_all_binaries(
    resp: Response,
    term: str = Query(None, description="A free text Azul search term"),
    after: str | None = Body(None, embed=True),
    num_binaries: int = Query(1000, description="Number of sha256s to return per request"),
    ctx: context.Context = Depends(qr.ctx),
):
    """Find binaries in the system based on search term.

    Returns only sha256 of matching binaries and is sorted alphabetically.

    Supports pagination through 'after' parameter from response. Final page will have 0 items.

    The pagination state doesn't 'timeout' however as the underlying database changes, entries may
    appear or disappear. For this reason, paging backwards to previous keys may provide different results.
    """
    try:
        data = binary_find_paginate.find_all_binaries(
            ctx,
            term=term,
            after=after,
            num_binaries=num_binaries,
        )
    except wrapper.InvalidSearchException as e:
        raise HTTPException(status_code=400, detail=e.args[0]) from e

    # If there is no data in the response and a term query was used check if the term query is valid.
    if (data.total is None or data.total == 0) and len(data.items) == 0 and term:
        # Check if grammar was appropriate
        model_valid_keys = binary_event.get_opensearch_binary_mapping(qr.writer)
        invalid_keys = validate_term_query(term, model_valid_keys)
        if len(invalid_keys) > 0:
            opt_s = "s" if len(invalid_keys) > 1 else ""
            raise HTTPException(
                status_code=422,
                detail=f"The following term query key{opt_s} could not be found: [{', '.join(invalid_keys)}]. "
                + f"Either the key{opt_s} are related to results and temporarily missing or the query is invalid.",
            )

    return qr.fr(ctx, data, resp)


@router.post("/v0/binaries/all/parents", response_model=qr.gr(bedr_binaries.EntityFindSimpleFamily), **qr.kw)
def find_all_parents(
    resp: Response,
    family_sha256: str,
    after: str | None = Body(None, embed=True),
    ctx: context.Context = Depends(qr.ctx),
):
    """Find parent binaries in the system based on family_sha256.

    Returns sha256, track_link, author_name and author_category of matching binaries and is sorted alphabetically.

    Supports pagination through 'after' parameter from response. Final page will have 0 items.

    The pagination state doesn't 'timeout' however as the underlying database changes, entries may
    appear or disappear. For this reason, paging backwards to previous keys may provide different results.
    """
    try:

        data = binary_find_paginate.find_all_family_binaries(
            ctx,
            family_sha256,
            is_parent=True,
            after=after,
        )

    except wrapper.InvalidSearchException as e:
        raise HTTPException(status_code=400, detail=e.args[0]) from e

    return qr.fr(ctx, data, resp)


@router.post("/v0/binaries/all/children", response_model=qr.gr(bedr_binaries.EntityFindSimpleFamily), **qr.kw)
def find_all_children(
    resp: Response,
    family_sha256: str,
    after: str | None = Body(None, embed=True),
    ctx: context.Context = Depends(qr.ctx),
):
    """Find child binaries in the system based on family_sha256.

    Returns sha256, track_link, author_name and author_category of matching binaries and is sorted alphabetically.

    Supports pagination through 'after' parameter from response. Final page will have 0 items.

    The pagination state doesn't 'timeout' however as the underlying database changes, entries may
    appear or disappear. For this reason, paging backwards to previous keys may provide different results.
    """
    try:

        data = binary_find_paginate.find_all_family_binaries(
            ctx,
            family_sha256,
            is_parent=False,
            after=after,
        )
    except wrapper.InvalidSearchException as e:
        raise HTTPException(status_code=400, detail=e.args[0]) from e

    return qr.fr(ctx, data, resp)


@router.get("/v0/binaries/model", response_model=qr.gr(bedr_binaries.EntityModel), **qr.kw)
def get_model(resp: Response, ctx: context.Context = Depends(qr.ctx)):
    """Return the flattened opensearch mapping for binaries to support user searches."""
    keys = binary_event.get_opensearch_binary_mapping(qr.writer)

    return qr.fr(ctx, {"keys": keys}, resp)


@router.get("/v0/binaries/autocomplete", response_model=qr.gr(AutocompleteContext), **qr.kw)
def find_autocomplete(resp: Response, term: str, offset: int, ctx: context.Context = Depends(qr.ctx)):
    """Return the model for binaries."""
    keys = binary_find.generate_autocomplete(term, offset)
    # although this looks like it doesn't do anything, it actually works around the
    # default dropping behaviour of unset values during the pydantic dump
    keys.type = keys.type
    return qr.fr(ctx, keys, resp)


@router.head("/v0/binaries/{sha256}", **qr.kw)
def check_metadata_exists(resp: Response, sha256: str, ctx: context.Context = Depends(qr.ctx_without_queries)):
    """Return 404 if entity not found, 200 if entity found."""
    data = binary_read.check_binaries(ctx, [sha256])[0]

    qr.set_security_headers(ctx, resp)

    if not data or not data["exists"]:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)
    return


@router.get("/v0/binaries/{sha256}", response_model=qr.gr(bedr_binaries.BinaryMetadata), **qr.kw)
def get_metadata(
    resp: Response,
    sha256: str,
    detail: list[bedr_binaries.BinaryMetadataDetail] = Query(
        [], description="Properties about binary that should be returned"
    ),
    author: str | None = Query(None, description="filter metadata to specific plugin author"),
    bucket_size: int = Query(
        100,
        description="Edit bucket size to get data if a query overflows the current bucket count "
        "(Buckets this affects are Features, Info, Streams(data) and Instances(Authors)).",
        gt=0,
        le=1000,
    ),
    ctx: context.Context = Depends(qr.ctx),
):
    """Return all simple metadata for the entity."""
    try:
        data = binary_summary.read(ctx, sha256, details=detail, author=author, bucket_size=bucket_size)
    except wrapper.InvalidSearchException as e:
        qr.set_security_headers(ctx, resp)
        raise HTTPException(status_code=400, detail=e.args[0]) from e

    if not data.documents.count:
        qr.set_security_headers(ctx, resp)
        raise HTTPException(status_code=404)

    return qr.fr(ctx, data, resp)


@router.get("/v0/binaries/{sha256}/new", response_model=qr.gr(bedr_binaries.BinaryDocuments), **qr.kw)
def get_has_newer_metadata(resp: Response, sha256: str, timestamp: str, ctx: context.Context = Depends(qr.ctx)):
    """Retrieve timestamp of newest result doc for entity."""
    data = binary_read.get_binary_newer(ctx, sha256, timestamp)
    return qr.fr(ctx, data, resp)


@router.get("/v0/binaries/similar/tlsh", response_model=qr.gr(bedr_binaries.SimilarFuzzyMatch), **qr.kw)
def get_similar_tlsh_binaries(
    resp: Response,
    tlsh: str = Query(description="TLSH hash to do comparison on"),
    max_matches: int = Query(20, description="Maximum number of matches to return"),
    ctx: context.Context = Depends(qr.ctx),
):
    """Return id and similarity score of entities with a similar TLSH hash."""
    data = {"matches": binary_similar.read_similar_from_tlsh(ctx, tlsh, max_matches)}
    return qr.fr(ctx, data, resp)


@router.get("/v0/binaries/similar/ssdeep", response_model=qr.gr(bedr_binaries.SimilarFuzzyMatch), **qr.kw)
def get_similar_ssdeep_binaries(
    resp: Response,
    ssdeep: str = Query(description="ssdeep fuzzyhash to do comparison on"),
    max_matches: int = Query(20, description="Maximum number of matches to return"),
    ctx: context.Context = Depends(qr.ctx),
):
    """Return id and similarity score of entities with a similar ssdeep fuzzyhash."""
    data = {"matches": binary_similar.read_similar_from_ssdeep(ctx, ssdeep, max_matches)}
    return qr.fr(ctx, data, resp)


@router.get("/v0/binaries/{sha256}/similar", response_model=qr.gr(bedr_binaries.SimilarMatch), **qr.kw)
def get_similar_feature_binaries(
    resp: Response, sha256: str, bt: BackgroundTasks, recalculate: bool = False, ctx: context.Context = Depends(qr.ctx)
):
    """Return info about similar entities."""
    gen = binary_similar.read_similar_from_features(ctx, sha256, recalculate=recalculate)
    data = next(gen)
    bt.add_task(lambda: next(gen))
    return qr.fr(ctx, data, resp)


@router.get("/v0/binaries/{sha256}/nearby", response_model=qr.gr(bedr_binaries.ReadNearby), **qr.kw)
def get_nearby_binaries(
    resp: Response,
    sha256: str,
    include_cousins: bedr_binaries.IncludeCousinsEnum = Query(
        bedr_binaries.IncludeCousinsEnum.Standard,
        description=".",
    ),
    ctx: context.Context = Depends(qr.ctx),
):
    """Return info about nearby entities."""
    if include_cousins == bedr_binaries.IncludeCousinsEnum.No:
        # Don't search for cousins
        data = binary_related.read_nearby(ctx, sha256)
    elif include_cousins == bedr_binaries.IncludeCousinsEnum.Small:
        # Search for cousins with some cousins but only one depth.
        data = binary_related.read_nearby(ctx, sha256, True, max_cousins=100, max_cousin_distance=1)
    elif include_cousins == bedr_binaries.IncludeCousinsEnum.Standard:
        # Search for cousins with the default max_cousins and max_cousin distance.
        data = binary_related.read_nearby(ctx, sha256, True)
    elif include_cousins == bedr_binaries.IncludeCousinsEnum.Large:
        # Search wider and for more cousins than default.
        data = binary_related.read_nearby(ctx, sha256, True, max_cousins=250, max_cousin_distance=4)
    else:
        raise HTTPException(status_code=400, detail=f"Provided value for {include_cousins=} is invalid.")

    if not data:
        qr.set_security_headers(ctx, resp)
        raise HTTPException(status_code=404)

    return qr.fr(ctx, data, resp)


@router.get("/v0/binaries/{sha256}/tags", response_model=qr.gr(bedr_binaries.ReadAllEntityTags), **qr.kw)
def get_binary_tags(resp: Response, sha256: str, ctx: context.Context = Depends(qr.ctx)):
    """Return tags for entity."""
    data = annotation.read_binary_tags(ctx, sha256)
    return qr.fr(ctx, {"items": data}, resp)


@router.post("/v0/binaries/{sha256}/tags/{tag}", **qr.kw)
def create_tag_on_binary(
    resp: Response,
    sha256: str,
    tag: str,
    security: str = Body(None, embed=True),
    ctx: context.Context = Depends(qr.ctx),
):
    """Attach a tag to an entity."""
    try:
        ctx.azsec.check_access(ctx.get_user_access().security.labels, security, raise_error=True)
    except SecurityParseException:
        raise ApiException(
            status_code=HTTP_422_UNPROCESSABLE_CONTENT,
            ref="Must provide valid security string.",
            internal="invalid_security_string",
        )
    except SecurityAccessException as e:
        raise ApiException(
            status_code=HTTP_422_UNPROCESSABLE_CONTENT,
            ref="security greater than user permissions",
            external="security being applied by the user is greater than the current users security."
            + f"because user: {str(e)}",
            internal="security_too_secure",
        ) from e

    qr.set_security_headers(ctx, resp, security)

    tag = dict(
        security=security,
        timestamp=pendulum.now().isoformat(),
        tag=tag,
        sha256=sha256,
    )

    try:
        annotation.create_binary_tags(qr.writer, ctx.user_info.username, [tag])
    except InvalidAnnotation as e:
        raise HTTPException(status_code=400, detail=repr(e))


@router.delete("/v0/binaries/{sha256}/tags/{tag}", response_model=qr.gr(bedr_binaries.AnnotationUpdated), **qr.kw)
def delete_tag_on_binary(resp: Response, sha256: str, tag: str, ctx: context.Context = Depends(qr.ctx)):
    """Delete a tag from an entity."""
    tag = unquote(tag) if tag else None
    try:
        data = annotation.delete_binary_tag(qr.writer, sha256, tag)
    except FileNotFoundError:
        raise HTTPException(status_code=404)
    finally:
        qr.set_security_headers(ctx, resp)

    return qr.fr(ctx, data, resp)


@router.get("/v0/binaries/{sha256}/statuses", response_model=qr.gr(bedr_binaries.Status), **qr.kw)
def get_binary_status(resp: Response, sha256: str, ctx: context.Context = Depends(qr.ctx)):
    """Status messages for all plugins that have run on the entity, including if they have timed out.

    :param sha256:
    :param ctx: Context
    :return: returns list of statuses
    """
    data = status.get_binary_status(ctx, sha256)
    if not data:
        qr.set_security_headers(ctx, resp)
        raise HTTPException(status_code=404)
    return qr.fr(ctx, bedr_binaries.Status(items=data), resp)


@router.get("/v0/binaries/{sha256}/events", response_model=qr.gr(bedr_binaries.OpensearchDocuments), **qr.kw)
def get_binary_documents(
    resp: Response,
    sha256: str,
    event_type: Optional[azm.BinaryAction] = Query(None, description="Filter out all but the specified event type."),
    size: int = Query(
        1000,
        description="Maximum number of events that will be returned.",
        gt=0,
        le=10000,
    ),
    ctx: context.Context = Depends(qr.ctx),
):
    """Get all documents from opensearch that match the provided filters.

    :param sha256: sha256 of the binary to get all the documents with the specified event_type for.
    :param event_type: Only get documents with the provided event_type.
    :param ctx: Context
    :return: returns list of documents returned by OpenSearch.
    """
    try:
        data = binary_event.get_binary_documents(ctx, sha256, event_type, size)
    except wrapper.InvalidSearchException as e:
        qr.set_security_headers(ctx, resp)
        raise HTTPException(status_code=400, detail=e.args[0]) from e

    if not data.items:
        qr.set_security_headers(ctx, resp)
        raise HTTPException(status_code=404)

    return qr.fr(ctx, data, resp)
