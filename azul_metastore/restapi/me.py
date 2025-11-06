"""User based API routes."""

from azul_bedrock.models_restapi.basic import UserAccess
from fastapi import APIRouter, Depends, Response

from azul_metastore import context
from azul_metastore.restapi.quick import qr

router = APIRouter()


@router.get("/v0/users/me/opensearch", response_model=UserAccess)
async def read_users_me(resp: Response, ctx: context.Context = Depends(qr.ctx)):
    """Return Opensearch access for current user."""
    qr.set_security_headers(ctx, resp)
    return ctx.get_user_access()
