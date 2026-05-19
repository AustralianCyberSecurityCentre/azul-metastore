"""Submit or trigger processing for binaries."""

from azul_bedrock import models_restapi
from azul_bedrock.exception_enums import ExceptionCodeEnum
from azul_bedrock.exceptions_bedrock import ApiException, BaseError
from azul_bedrock.models_restapi import binaries_download as bedr_binaries_down
from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Path,
    Request,
    Response,
)
from starlette.status import HTTP_404_NOT_FOUND

from azul_metastore import context
from azul_metastore.query import status
from azul_metastore.query.binary2 import binary_submit
from azul_metastore.restapi.quick import qr

router = APIRouter()


@router.post(
    "/v0/binaries/source/download",
    response_model=qr.gr(bedr_binaries_down.DownloadResponse),
    responses={
        422: {"model": BaseError, "description": "Invalid file"},
    },
    **qr.kw,
)
async def submit_binary_download_request(
    request: Request,
    resp: Response,
    # sha256 to Body to download
    sha256: str = Body(description="Sha256 of file to download from external source.", embed=True),
    security: str = Body(
        "",
        description="Space separated list of security labels e.g 'OFFICIAL TLP:CLEAR'",
        embed=True,
    ),
    # source submission
    source_id: str = Body(default=None, description="Source/grouping to submit the file into", embed=True),
    references: dict[str, str] = Body(
        default=dict(),
        description="Reference key value pairs.",
        embed=True,
    ),
    settings: dict[str, str] = Body(
        default=dict(),
        description="Settings key value pairs.",
        embed=True,
    ),
    ctx: context.Context = Depends(qr.ctx),
):
    """Submit a request to download a file from a remote source.

    NOTE - this source will be at the classification of the system.
    Ensure you don't submit sha256's that are classified at a higher classification.
    """
    # Validate user supplied label
    security = ctx.validate_user_security(security)

    try:
        user = request.state.user_info.username

        qr.set_security_headers(ctx, resp, security)

        result = await binary_submit.submit_download_request(
            ctx=ctx,
            priv_ctx=qr.writer,
            sha256=sha256,
            source=source_id,
            security=security,
            user=user,
            references=references,
            submit_settings=settings,
        )
        return qr.fr(ctx, result, resp)
    except (HTTPException, ApiException) as e:
        qr.set_security_headers(ctx, resp, security, ex=e)
        raise


@router.get("/v0/binaries/source/download/{sha256}", response_model=qr.gr(list[models_restapi.StatusEvent]), **qr.kw)
async def get_binary_download_request_status(
    resp: Response,
    # sha256 to attempt to download
    sha256: str = Path(..., pattern="[a-fA-F0-9]{64}", description="Sha256 of file that download was requested for."),
    ctx: context.Context = Depends(qr.ctx),
):
    """Get the status per plugin for a download request that was submitted."""
    result = status.get_binary_status_for_download_plugins(ctx, sha256)
    if len(result) == 0:
        raise ApiException(
            status_code=HTTP_404_NOT_FOUND,
            internal=ExceptionCodeEnum.MetastoreDownloadRequestNotMade,
            parameters={"sha256": sha256},
        ) from None
    return qr.fr(ctx, result, resp)
