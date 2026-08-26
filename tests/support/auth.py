"""Create mock auth tokens for testing."""

from azul_bedrock.models_auth import ApiAccessEnum

import datetime
from pydantic import BaseModel
import jwt
from azul_bedrock.datastore import Credentials, CredentialFormat

from azul_metastore.encoders import base_encoder

# This pre-shared secret matches the provided docker-compose Opensearch cluster for local testing.
_SECRET = "secret.secret.secret.secret.secret.secret."


def gen_token(markings: list[str], user: str):
    """Generate a tests jwt token using a preshared secret."""
    markings.append(base_encoder.S_ANY)
    return jwt.encode(
        {
            "roles": ["azul_read"] + markings,
            "sub": user,
            "iss": "https://localhost",
            "iat": datetime.datetime.now() - datetime.timedelta(weeks=1),
            "nbf": datetime.datetime.now() - datetime.timedelta(weeks=1),
            "exp": datetime.datetime.now() + datetime.timedelta(weeks=1),
        },
        _SECRET,
        algorithm="HS256",
    )


def get_roles(token: str):
    token = jwt.decode(token, key=_SECRET, algorithms="HS256")
    return token["roles"]


class CredsWithAccess(BaseModel):
    """Credentials with api_access."""

    creds: Credentials
    api_access: list[ApiAccessEnum]


class Auth:
    users: dict[str, CredsWithAccess] = {}
    users["low"] = user_low = CredsWithAccess(
        creds=Credentials(
            unique="low",
            format=CredentialFormat.jwt,
            token=gen_token(markings=["LOW"], user="low"),
        ),
        api_access=[ApiAccessEnum.All],
    )
    users["med"] = user_med = CredsWithAccess(
        creds=Credentials(
            unique="med",
            format=CredentialFormat.jwt,
            token=gen_token(markings=["LOW", "MEDIUM", "REL:APPLE"], user="med"),
        ),
        api_access=[ApiAccessEnum.All],
    )
    users["high"] = user_high = CredsWithAccess(
        creds=Credentials(
            unique="high",
            format=CredentialFormat.jwt,
            token=gen_token(markings=["LOW", "MEDIUM", "MOD1", "REL:APPLE"], user="high"),
        ),
        api_access=[ApiAccessEnum.All],
    )
    users["high_org2"] = user_high_org2 = CredsWithAccess(
        creds=Credentials(
            unique="high_org2",
            format=CredentialFormat.jwt,
            token=gen_token(markings=["LOW", "MEDIUM", "HIGH", "MOD1", "REL:BEE"], user="high_org2"),
        ),
        api_access=[ApiAccessEnum.All],
    )
    users["high_all"] = user_high_all = CredsWithAccess(
        creds=Credentials(
            unique="high_all",
            format=CredentialFormat.jwt,
            token=gen_token(
                markings=[
                    "LOW",
                    "LOW: LY",
                    "MEDIUM",
                    "HIGH",
                    "TOP HIGH",
                    "MOD1",
                    "MOD2",
                    "MOD3",
                    "HANOVERLAP",
                    "OVER",
                    "REL:APPLE",
                    "REL:BEE",
                    "REL:CAR",
                ],
                user="high_all",
            ),
        ),
        api_access=[ApiAccessEnum.All],
    )
    users["high_all_upload_api_only"] = user_high_all_upload_api = CredsWithAccess(
        creds=Credentials(
            unique="high_all",
            format=CredentialFormat.jwt,
            token=gen_token(
                markings=[
                    "LOW",
                    "LOW: LY",
                    "MEDIUM",
                    "HIGH",
                    "TOP HIGH",
                    "MOD1",
                    "MOD2",
                    "MOD3",
                    "HANOVERLAP",
                    "OVER",
                    "REL:APPLE",
                    "REL:BEE",
                    "REL:CAR",
                ],
                user="high_all",
            ),
        ),
        api_access=[ApiAccessEnum.BinarySourceUpload],
    )
    users["high_all_feature_apis"] = user_high_all_features_apis = CredsWithAccess(
        creds=Credentials(
            unique="high_all",
            format=CredentialFormat.jwt,
            token=gen_token(
                markings=[
                    "LOW",
                    "LOW: LY",
                    "MEDIUM",
                    "HIGH",
                    "TOP HIGH",
                    "MOD1",
                    "MOD2",
                    "MOD3",
                    "HANOVERLAP",
                    "OVER",
                    "REL:APPLE",
                    "REL:BEE",
                    "REL:CAR",
                ],
                user="high_all",
            ),
        ),
        api_access=[ApiAccessEnum.FeatureSearch],
    )
    users["anonymoose"] = user_anonymoose = CredsWithAccess(
        creds=Credentials(
            unique="anonymoose",
            format=CredentialFormat.jwt,
            token=gen_token(markings=["OFFICIAL"], user="anonymoose"),
        ),
        api_access=[ApiAccessEnum.All],
    )
