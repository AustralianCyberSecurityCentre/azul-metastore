"""Create mock auth tokens for testing."""

import datetime

import jwt

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


class Auth:
    users = {}
    users["low"] = user_low = {
        "unique": "low",
        "format": "jwt",
        "token": gen_token(markings=["LOW"], user="low"),
    }
    users["med"] = user_med = {
        "unique": "med",
        "format": "jwt",
        "token": gen_token(markings=["LOW", "MEDIUM", "REL:APPLE"], user="med"),
    }
    users["high"] = user_high = {
        "unique": "high",
        "format": "jwt",
        "token": gen_token(markings=["LOW", "MEDIUM", "MOD1", "REL:APPLE"], user="high"),
    }
    users["high_org2"] = user_high_org2 = {
        "unique": "high_org2",
        "format": "jwt",
        "token": gen_token(markings=["LOW", "MEDIUM", "HIGH", "MOD1", "REL:BEE"], user="high_org2"),
    }
    users["high_all"] = user_high_all = {
        "unique": "high_all",
        "format": "jwt",
        "token": gen_token(
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
    }
    users["anonymoose"] = user_anonymoose = {
        "unique": "anonymoose",
        "format": "jwt",
        "token": gen_token(markings=["OFFICIAL"], user="anonymoose"),
    }
