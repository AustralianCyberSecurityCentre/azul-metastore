"""Metastore security control implementation for Opensearch 1.x and 2.x."""

import copy

import httpx
from azul_bedrock import exceptions_bedrock, settings
from azul_bedrock.exception_enums import ExceptionCodeEnum


def get_user_account(user_auth: dict) -> dict:
    """Get user account information from opensearch."""
    s = settings.get_opensearch()
    # opensearch path to get account information
    api_path = "_plugins/_security/api/account"

    raw = copy.deepcopy(user_auth)
    if "http_auth" in raw:
        raw["auth"] = httpx.BasicAuth(*raw.pop("http_auth"))
    try:
        endpoint = f"{s.opensearch_url}/{api_path}"
        resp = httpx.get(endpoint, verify=s.certificate_verification, timeout=10, **raw)
        if resp.status_code not in {200, 201}:
            raise exceptions_bedrock.BaseAzulException(
                internal=ExceptionCodeEnum.MetastoreOpensearchCantGetUserAccountInner,
                parameters={"status_code": str(resp.status_code), "response_text": str(resp.content)},
            )
        return resp.json()
    except Exception as e:
        raise exceptions_bedrock.BaseAzulException(
            ref=f"could not get user account: {str(e)}",
            internal=ExceptionCodeEnum.MetastoreOpensearchCantGetUserAccount,
            parameters={"inner_exception": str(e)},
        ) from e
