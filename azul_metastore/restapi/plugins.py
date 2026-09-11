"""Routes for entity data queries."""

from azul_bedrock.exception_enums import ExceptionCodeEnum
from azul_bedrock.exceptions_bedrock import ApiException
from azul_bedrock.models_restapi import ApiAccessEnum
from azul_bedrock.models_restapi import Author as PluginAuthor
from azul_bedrock.models_restapi import plugins as bedr_plugins
from fastapi import APIRouter, Depends, Response

from azul_metastore import context
from azul_metastore.query import plugin
from azul_metastore.query.binary2 import binary_read
from azul_metastore.restapi.quick import can_user_access_api_wrapper, qr

router = APIRouter()


@router.get("/v0/plugins", response_model=qr.gr(list[bedr_plugins.LatestPluginWithVersions]), **qr.kw)
def get_all_plugins(
    resp: Response,
    ctx: context.Context = Depends(can_user_access_api_wrapper(ApiAccessEnum.PluginSearch)),
):
    """Read names and versions of all registered plugins."""
    data = plugin.get_all_plugins(ctx)
    if not data:
        qr.set_security_headers(ctx, resp)
        raise ApiException(status_code=404, internal=ExceptionCodeEnum.MetastoreNoPluginsInAzul)
    return qr.fr(ctx, [d.model_dump(mode="json") for d in data], resp)


@router.get("/v0/plugins/status", response_model=qr.gr(list[bedr_plugins.PluginStatusSummary]), **qr.kw)
def get_all_plugin_statuses(
    resp: Response,
    ctx: context.Context = Depends(can_user_access_api_wrapper(ApiAccessEnum.PluginSearch)),
):
    """Read names and versions of all registered plugins.

    Note - the status count is inaccurate because it doesn't filter out duplicates.
    A duplicate is where the same binary is submitted to a plugin with a different path.
    """
    data = plugin.get_all_plugin_latest_activity(ctx)
    if not data:
        qr.set_security_headers(ctx, resp)
        raise ApiException(status_code=404, internal=ExceptionCodeEnum.MetastoreNoPluginStatusesInAzul)
    return qr.fr(ctx, [d.model_dump(mode="json") for d in data], resp)


@router.get("/v0/plugins/{name}/versions/{version}", response_model=qr.gr(bedr_plugins.PluginInfo), **qr.kw)
def get_plugin(
    resp: Response,
    name: str,
    version: str,
    ctx: context.Context = Depends(can_user_access_api_wrapper(ApiAccessEnum.PluginSearch)),
):
    """Read data for one plugin version."""
    data = {
        # count entity results from this author
        "num_entities": binary_read.get_author_stats(ctx, name, version),
        # get basic plugin info
        "plugin": plugin.get_plugin(ctx, name, version),
        # get currently running information (status records)
        # get last x failures information (status records)
        "status": plugin.get_author_stats(ctx, name, version),
    }
    # no trace of plugin
    if not data["plugin"]:
        qr.set_security_headers(ctx, resp)
        raise ApiException(status_code=404, internal=ExceptionCodeEnum.MetastorePluginNotInAzul)
    return qr.fr(ctx, data, resp)


@router.get("/v0/plugins/download", response_model=qr.gr(list[PluginAuthor]), **qr.kw)
def get_download_plugins(
    resp: Response, ctx: context.Context = Depends(can_user_access_api_wrapper(ApiAccessEnum.PluginSearch))
):
    """Find all plugins that are able to download files."""
    data = plugin.get_download_plugins(ctx)
    if not data:
        qr.set_security_headers(ctx, resp)
        raise ApiException(status_code=404, internal=ExceptionCodeEnum.MetastoreNoDownloadPluginsInAzul)
    return qr.fr(ctx, data, resp)


@router.get("/v0/plugins/summary", response_model=qr.gr(list[plugin.PluginSummary]), **qr.kw)
def get_plugin_summary(
    resp: Response,
    ctx: context.Context = Depends(can_user_access_api_wrapper(ApiAccessEnum.PluginSearch)),
):
    """Return all plugin data."""
    static = plugin.get_plugin_summary_static(ctx)
    dynamic = plugin.get_plugin_summary_dynamic(ctx)

    combined_data = {}
    for plugin_ in static:
        combined_data[plugin_.name] = plugin_
    for plugin_ in dynamic:
        if plugin_.name not in combined_data:
            combined_data[plugin_.name] = plugin_
            continue
        
        combined_data[plugin_.name].last_completion = plugin_.last_completion
        combined_data[plugin_.name].completion_count = plugin_.completion_count
        combined_data[plugin_.name].error_count = plugin_.error_count
        combined_data[plugin_.name].completion_percent = plugin_.completion_percent

    if not static and not dynamic:
        qr.set_security_headers(ctx,resp)
        raise ApiException(status_code=404, internal=ExceptionCodeEnum.MetastoreNoPluginsInAzul)

    return qr.fr(ctx, combined_data.values(), resp)


@router.get("/v0/plugins/summary/static", response_model=qr.gr(list[plugin.PluginSummary]), **qr.kw)
def get_plugin_summary_static(
    resp: Response,
    ctx: context.Context = Depends(can_user_access_api_wrapper(ApiAccessEnum.PluginSearch)),
):
    """Returns the static values of all plugins. Covers: Name, Version, Security, Descriptions, and Feature count."""
    data = plugin.get_plugin_summary_static(ctx)
    if not data:
        qr.set_security_headers(ctx, resp)
        raise ApiException(status_code=404, internal=ExceptionCodeEnum.MetastoreNoPluginsInAzul)

    return qr.fr(ctx, data, resp)


@router.get("/v0/plugins/summary/dynamic", response_model=qr.gr(list[plugin.PluginSummary]), **qr.kw)
def get_plugin_summary_dynamic(
    resp: Response,
    ctx: context.Context = Depends(can_user_access_api_wrapper(ApiAccessEnum.PluginSearch)),
):
    """Returns the dynamic values of all plugins. Covers: Last completed, Completed, Error, and Completed percent."""
    data = plugin.get_plugin_summary_dynamic(ctx)
    if not data:
        qr.set_security_headers(ctx, resp)
        raise ApiException(status_code=404, internal=ExceptionCodeEnum.MetastoreNoPluginsInAzul)

    return qr.fr(ctx, data, resp)
