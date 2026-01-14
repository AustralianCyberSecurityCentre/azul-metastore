"""Fastapi helpers for constructing queries."""

import logging
from typing import Optional

import cachetools
from azul_bedrock.models_auth import UserInfo
from azul_bedrock.models_restapi import basic as bedr_basic
from azul_security.exceptions import SecurityAccessException
from fastapi import HTTPException, Query, Request, Response
from pydantic import create_model
from starlette.status import HTTP_401_UNAUTHORIZED

from azul_metastore import context, settings
from azul_metastore.common import memcache, search_data

logger = logging.getLogger(__name__)


@cachetools.cached(cache=memcache.get_lru_cache("restapi_get_base"))
def _get_base() -> context.Context:
    """Return the general context object, cache so it only gets created once."""
    logger.info("metastore - create commonly used objects")
    return context.get_general_context()


@cachetools.cached(
    cache=memcache.get_ttl_cache("restapi_get_subctx"),
    key=lambda x, y, z, f: (x.credentials.unique + "." + ".".join(y) + "." + ".".join(z) + "." + ".".join(f or [])),
)
def _get_subctx_cached(
    user_info: UserInfo,
    security_exclude: list[str],
    security_include: list[str] = None,
    security_filter: str = None,
) -> context.Context:
    """Get the context for making queries to Opensearch."""
    return _get_subctx(user_info, security_exclude, security_include, security_filter)


def _get_subctx(
    user_info: UserInfo,
    security_exclude: list[str],
    security_include: list[str],
    security_filter: str,
) -> context.Context:
    """Get the context for making queries to Opensearch and get a cached version if it's available."""
    security_exclude = [x.upper() for x in security_exclude]  # FUTURE use security module for this.
    security_include = [i.upper() for i in security_include]

    ctx = _get_base().copy_with(
        user_info=user_info,
        sd=search_data.SearchData(
            credentials=user_info.credentials.model_dump(),
            security_exclude=security_exclude,
            security_include=security_include,
            security_filter=security_filter,
            enable_log_es_queries=settings.get().log_opensearch_queries,
        ),
    )

    # verify that we have minimum required access
    try:
        ctx.get_user_access()
    except SecurityAccessException as e:
        raise HTTPException(status_code=HTTP_401_UNAUTHORIZED, detail=str(e))
    return ctx


class QuickRefs:
    """Collects together a few very commonly used pieces of data used by restapi endpoints for metastore."""

    models = {}

    @classmethod
    def gen_response(cls, _type):
        """Response objects are generated dynamically as I was sick of wrapping objects in (data:object,meta:dict)."""
        name = f"Response:{str(_type)}"
        if name not in cls.models:
            cls.models[name] = create_model(name, __base__=bedr_basic.Response, data=(_type, None))
        return cls.models[name]

    @classmethod
    def set_security_headers(
        cls,
        ctx: context.Context,
        response: Optional[Response],
        security_label: Optional[str] = None,
        ex: Optional[HTTPException] = None,
    ):
        """Configures security headers for the response for this request."""
        if security_label is None:
            security_label = ctx.get_user_current_security()
        # Raised HTTPResponses might not encode a regular response. Do this ourselves:
        if ex is not None:
            if ex.headers is None:
                ex.headers = dict()
            ex.headers["x-azul-security"] = security_label
        elif response is not None:
            response.headers["x-azul-security"] = security_label
            # Now that a security label has been set manually, remove the defaulted marker
            # This always passes even if this header doesn't exist
            del response.headers["x-azul-security-defaulted"]
        else:
            raise Exception("Logic error - should be setting response and/or exception!")

    @classmethod
    def format_response(cls, ctx: context.Context, data, response: Response):
        """Wrap the responses in a common model and add information about the request."""
        meta = bedr_basic.Meta()

        # add opensearch query info to response
        if ctx.sd is not None and ctx.sd.enable_capture_es_queries:
            meta.queries = ctx.sd.captured_es_queries

        # get current security context for the query
        meta.security = ctx.get_user_current_security()
        meta.sec_filter = ctx.sd.security_filter
        # ensure response has a http header with accurate security info
        cls.set_security_headers(ctx, response, meta.security)

        return bedr_basic.Response(data=data, meta=meta)

    def subctx(
        self,
        user_info: UserInfo,
        security_exclude: list[str],
        security_include: list[str],
        security_filter: str,
        no_cache: bool,
    ):
        """Return ctx for current user (overwriteable)."""
        if no_cache:
            ctx = _get_subctx(user_info, security_exclude, security_include, security_filter)
        else:
            ctx = _get_subctx_cached(user_info, security_exclude, security_include, security_filter)
        # remove state gathered on last request (e.g. num opensearch queries)
        ctx.clear_state()
        return ctx

    def ctx(
        self,
        request: Request,
        response: Response,
        security_exclude: list[str] = Query([], alias="x", description="Exclude these security labels during queries"),
        security_include: list[str] = Query(
            [], alias="i", description="Include these RELs for AND search in opensearch during queries"
        ),
        security_filter: str = Query("", alias="f", description="Filter type for releasability filter"),
        include_queries: bool = Query(
            False, alias="include_queries", description="Include all Opensearch queries run during request."
        ),
    ) -> context.Context:
        """Return ctx for current user."""
        try:
            user_info = request.state.user_info
        except AttributeError as e:
            raise Exception("user_info is not available on request.state") from e

        # If we are enabling es queries we should also bypass the cache so we need the value now.
        ctx = self.subctx(user_info, security_exclude, security_include, security_filter, no_cache=include_queries)
        ctx.sd.enable_capture_es_queries = include_queries

        # Configure initial security headers for this context in case it doesn't get set later.
        response.headers.append("x-azul-security", ctx.azsec.get_default_security())
        response.headers.append("x-azul-security-defaulted", "true")

        return ctx

    def ctx_without_queries(
        self,
        request: Request,
        response: Response,
        security_exclude: list[str] = Query([], alias="x", description="Exclude these security labels during queries"),
        security_include: list[str] = Query(
            [], alias="i", description="Include these RELs for AND search in opensearch during queries"
        ),
        security_filter: str = Query("", alias="f", description="Filter type for releasability filter"),
        include_queries: bool = Query(False, include_in_schema=False),
    ) -> context.Context:
        """Return ctx for current user (add's alias for include_queries)."""
        return self.ctx(request, response, security_exclude, security_include, security_filter, include_queries)

    gr = gen_response
    fr = format_response
    kw = {
        "response_model_exclude_unset": True,
    }

    @property
    def writer(self) -> context.Context:
        """Get the context object for the writer user."""
        return context.get_writer_context()


qr = QuickRefs()
