"""Routes for entity data queries."""

from azul_bedrock.models_restapi import plugins as bedr_plugins
from fastapi import APIRouter, Depends, HTTPException, Response

from azul_metastore import context
from azul_metastore.query import plugin
from azul_metastore.query.binary2 import binary_read
from azul_metastore.restapi.quick import qr

router = APIRouter()


@router.get("/v0/plugins", response_model=qr.gr(list[bedr_plugins.LatestPluginWithVersions]), **qr.kw)
def get_all_plugins(
    resp: Response,
    ctx: context.Context = Depends(qr.ctx),
):
    """Read names and versions of all registered plugins."""
    data = plugin.get_all_plugins(ctx)
    if not data:
        qr.set_security_headers(ctx, resp)
        raise HTTPException(status_code=404)
    return qr.fr(ctx, [d.model_dump(mode="json") for d in data], resp)


@router.get("/v0/plugins/status", response_model=qr.gr(list[bedr_plugins.PluginStatusSummary]), **qr.kw)
def get_all_plugin_statuses(
    resp: Response,
    ctx: context.Context = Depends(qr.ctx),
):
    """Read names and versions of all registered plugins.

    Note - the status count is inaccurate because it doesn't filter out duplicates.
    A duplicate is where the same binary is submitted to a plugin with a different path.
    """
    data = plugin.get_all_plugin_latest_activity(ctx)
    if not data:
        qr.set_security_headers(ctx, resp)
        raise HTTPException(status_code=404)
    return qr.fr(ctx, [d.model_dump(mode="json") for d in data], resp)


@router.get("/v0/plugins/{name}/versions/{version}", response_model=qr.gr(bedr_plugins.PluginInfo), **qr.kw)
def get_plugin(resp: Response, name: str, version: str, ctx: context.Context = Depends(qr.ctx)):
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
        raise HTTPException(status_code=404)
    return qr.fr(ctx, data, resp)
