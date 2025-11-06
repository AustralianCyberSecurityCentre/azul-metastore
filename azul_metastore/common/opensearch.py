"""Metastore security control implementation for Opensearch 1.x and 2.x."""

import copy

import httpx

from azul_metastore import settings


def get_user_account(user_auth: dict) -> dict:
    """Get user account information from opensearch."""
    s = settings.get()
    # opensearch path to get account information
    api_path = "_plugins/_security/api/account"

    e = Exception("generic")
    raw = copy.deepcopy(user_auth)
    if "http_auth" in raw:
        raw["auth"] = httpx.BasicAuth(*raw.pop("http_auth"))
    try:
        endpoint = f"{s.opensearch_url}/{api_path}"
        resp = httpx.get(endpoint, verify=s.certificate_verification, timeout=10, **raw)
        if resp.status_code not in {200, 201}:
            raise Exception(resp.status_code, resp.content)
        return resp.json()
    except Exception as _e:
        e = _e
    raise Exception(f"could not get user account: {str(e)}") from e
