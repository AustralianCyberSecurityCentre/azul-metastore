"""Routes for feature data queries."""

import pendulum
from azul_bedrock.exceptions import ApiException
from azul_bedrock.models_restapi import features as bedr_features
from azul_security import exceptions
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Response
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from azul_metastore import context
from azul_metastore.encoders.annotation import InvalidAnnotation
from azul_metastore.query import annotation
from azul_metastore.query import plugin as plg
from azul_metastore.query.binary2 import binary_feature
from azul_metastore.restapi.quick import qr

router = APIRouter()


@router.post(
    "/v0/features/values/counts", response_model=qr.gr(dict[str, bedr_features.FeatureMulticountRet]), **qr.kw
)
def count_values_in_features(
    resp: Response,
    items: list[str] = Body([], embed=True),
    skip_count: bool = Query(False),
    author: str = Query(""),
    author_version: str = Query(""),
    ctx: context.Context = Depends(qr.ctx),
):
    """Count number of unique values for features."""
    ret = {}
    filters = (
        []
        + ([{"term": {"author.name": author}}] if author else [])
        + ([{"term": {"author.version": author_version}}] if author_version else [])
    )
    for result in binary_feature.count_values_in_features(ctx, items, skip_count=skip_count, filters=filters):
        ret[result.name] = result
    return qr.fr(ctx, ret, resp)


@router.post(
    "/v0/features/entities/counts", response_model=qr.gr(dict[str, bedr_features.FeatureMulticountRet]), **qr.kw
)
def count_binaries_in_features(
    resp: Response,
    items: list[str] = Body([], embed=True),
    skip_count: bool = Query(False),
    author: str = Query(""),
    author_version: str = Query(""),
    ctx: context.Context = Depends(qr.ctx),
):
    """Count number of unique entities for features."""
    ret = {}
    filters = (
        []
        + ([{"term": {"author.name": author}}] if author else [])
        + ([{"term": {"author.version": author_version}}] if author_version else [])
    )
    for result in binary_feature.count_binaries_with_feature_names(ctx, items, skip_count=skip_count, filters=filters):
        ret[result.name] = result
    return qr.fr(ctx, ret, resp)


@router.post(
    "/v0/features/values/entities/counts",
    response_model=qr.gr(dict[str, dict[str, bedr_features.ValueCountRet]]),
    **qr.kw,
)
def count_binaries_in_featurevalues(
    resp: Response,
    items: list[bedr_features.ValueCountItem] = Body([], embed=True),
    skip_count: bool = Query(False),
    author: str = Query(""),
    author_version: str = Query(""),
    ctx: context.Context = Depends(qr.ctx),
):
    """Count unique entities for multiple feature values."""
    ret = {}
    filters = (
        []
        + ([{"term": {"author.name": author}}] if author else [])
        + ([{"term": {"author.version": author_version}}] if author_version else [])
    )
    for result in binary_feature.count_binaries_with_feature_values(
        ctx, items, skip_count=skip_count, filters=filters
    ):
        ret.setdefault(result.name, {})[result.value] = result
    return qr.fr(ctx, ret, resp)


@router.post(
    "/v0/features/values/parts/entities/counts",
    response_model=qr.gr(dict[str, dict[str, bedr_features.ValuePartCountRet]]),
    **qr.kw,
)
def count_binaries_in_featurevalueparts(
    resp: Response,
    items: list[bedr_features.ValuePartCountItem] = Body([], embed=True),
    author: str = Query(""),
    author_version: str = Query(""),
    skip_count: bool = Query(False),
    ctx: context.Context = Depends(qr.ctx),
):
    """Count unique entities for multiple value parts."""
    ret = {}
    filters = (
        []
        + ([{"term": {"author.name": author}}] if author else [])
        + ([{"term": {"author.version": author_version}}] if author_version else [])
    )
    for result in binary_feature.count_binaries_with_part_values(ctx, items, skip_count=skip_count, filters=filters):
        ret.setdefault(result.value, {})[result.part] = result
    return qr.fr(ctx, ret, resp)


@router.get("/v0/features/all/tags", response_model=qr.gr(bedr_features.ReadFeatureValueTags), **qr.kw)
def get_all_feature_value_tags(resp: Response, ctx: context.Context = Depends(qr.ctx)):
    """Attach a tag to a specific feature value."""
    return qr.fr(ctx, annotation.read_all_feature_value_tags(ctx), resp)


@router.get("/v0/features/tags/{tag}", response_model=qr.gr(bedr_features.ReadFeatureTagValues), **qr.kw)
def get_feature_values_in_tag(resp: Response, tag: str, ctx: context.Context = Depends(qr.ctx)):
    """Attach a tag to a specific feature value."""
    return qr.fr(ctx, annotation.read_feature_values_for_tag(ctx, tag), resp)


@router.post("/v0/features/tags/{tag}", **qr.kw)
def create_feature_value_tag(
    resp: Response,
    tag: str,
    feature: str,
    value: str,
    security: str = Body(None, embed=True),
    ctx: context.Context = Depends(qr.ctx),
):
    """Attach a tag to a specific feature value."""
    try:
        security = ctx.azsec.string_normalise(security)
    except exceptions.SecurityException:
        raise ApiException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            ref="security was not valid",
            external=f"security was not valid ({security})",
            internal="upload_bad_security",
        )

    tag = dict(
        security=security,
        timestamp=pendulum.now().isoformat(),
        feature_name=feature,
        feature_value=value,
        tag=tag,
    )
    try:
        annotation.create_feature_value_tags(qr.writer, ctx.user_info.username, [tag])
        qr.set_security_headers(ctx, resp)
    except InvalidAnnotation as e:
        e = HTTPException(status_code=400, detail=repr(e))
        qr.set_security_headers(ctx, resp, ex=e)
        raise e


@router.delete("/v0/features/tags/{tag}", **qr.kw)
def delete_feature_value_tag(
    resp: Response, feature: str, value: str, tag: str, ctx: context.Context = Depends(qr.ctx)
):
    """Remove a tag from a specific feature value."""
    try:
        annotation.delete_feature_value_tag(qr.writer, feature, value, tag)
        qr.set_security_headers(ctx, resp)
    except FileNotFoundError:
        e = HTTPException(status_code=404)
        qr.set_security_headers(ctx, resp, ex=e)
        raise e


@router.get("/v0/features", response_model=qr.gr(bedr_features.Features), **qr.kw)
def find_features(
    resp: Response, author: str = Query(""), author_version: str = Query(""), ctx: context.Context = Depends(qr.ctx)
):
    """Return features present in system."""
    filters = (
        []
        + ([{"term": {"author.name": author}}] if author else [])
        + ([{"term": {"author.version": author_version}}] if author_version else [])
    )
    # empty list of features is also a 200 but indicates no plugins yet registered in opensearch
    data = plg.find_features(ctx, filters=filters)
    return qr.fr(ctx, {"items": data}, resp)


@router.post("/v0/features/feature/{feature}", response_model=qr.gr(bedr_features.ReadFeatureValues), **qr.kw)
def find_values_in_feature(
    resp: Response,
    feature: str,
    term: str = Query("", description="Value must contain this term"),
    sort_asc: bool = Query(
        True,
        description="Sort results in ascending order.",
    ),
    case_insensitive: bool = Query(False, description="Term search should be case insensitive."),
    author: str = Query("", description="Name of plugin that produced this."),
    author_version: str = Query("", description="Version of plugin that produced this."),
    num_values: int = Query(500, description="Number of feature values to return per request"),
    after: str | None = Body(None, embed=True),
    ctx: context.Context = Depends(qr.ctx),
):
    """Return a set of values for the specified feature.

    Supports pagination through 'after' parameter from response. Final page may have 0 items.
    Return value for is_search_complete will return true if there are no more values for the feature.
    """
    filters = (
        []
        + ([{"term": {"author.name": author}}] if author else [])
        + ([{"term": {"author.version": author_version}}] if author_version else [])
    )
    data = binary_feature.find_feature_values(
        ctx,
        feature,
        term=term,
        sort_asc=sort_asc,
        case_insensitive=case_insensitive,
        filters=filters,
        num_values=num_values,
        after=after,
    )
    if not data:
        e = HTTPException(status_code=404)
        qr.set_security_headers(ctx, resp, ex=e)
        raise e
    return qr.fr(ctx, data, resp)
