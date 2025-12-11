"""Dangerous system actions we don't want in a deployed code base."""

import functools

from azul_bedrock.models_auth import Credentials, UserInfo
from fastapi import Depends, FastAPI, Request
from starlette.testclient import TestClient

from azul_metastore import context, settings
from azul_metastore.common import search_data
from tests.support import auth


def set_token(request: Request) -> UserInfo:
    """Ignore input and return static token."""
    user = request.headers.get("x-test-user", "high_all")
    extra_roles = []
    if user == "writer":
        creds = settings.get_writer_creds()
        extra_roles = ["admin"]
    elif user in auth.Auth.users:
        creds = auth.Auth.users[user]
    else:
        raise Exception(f"unknown user {user}")
    data = {
        "exp": -1,
        "token_type": "Bearer",
        "preferred_username": user,
        "org": "test",
        "roles": ["validated"] + extra_roles,
    }
    request.state.user_info = UserInfo(
        username=user,
        org="test",
        email="",
        roles=["validated"] + extra_roles,
        decoded=data,
        credentials=Credentials(**creds),
        unique_id=user,
    )
    return request.state.user_info


def get_app():
    """Get a client for testing metastore routes."""
    from azul_metastore.restapi import (
        binaries,
        binaries_data,
        binaries_submit,
        features,
        me,
        plugins,
        purge,
        sources,
        statistics,
    )

    app = FastAPI(
        title="Azul",
        version="test",
        openapi_url=f"/api/openapi.json",
        docs_url=None,
        redoc_url=None,
        dependencies=[Depends(set_token)],
    )
    app.include_router(binaries_submit.router)
    app.include_router(binaries_data.router)
    app.include_router(binaries.router)
    app.include_router(features.router)
    app.include_router(me.router)
    app.include_router(plugins.router)
    app.include_router(purge.router)
    app.include_router(sources.router)
    app.include_router(statistics.router)
    client = TestClient(app)

    return client


class System:
    def __init__(self, partition: str):
        # self.test_index = f"azul.{partition}.*"
        self.index_base = f"azul.{partition}*"
        self.index_opened = f"azul.o.{partition}*"
        self.index_closed = f"azul.x.{partition}*"
        self.patterns = [self.index_base, self.index_opened, self.index_closed]

    def flush(self):
        """Index all documents to allow search."""
        # self.es_admin.indices.refresh(index=self.test_index)
        self.writer.refresh()

    @property
    def es_admin(self):
        """Get admin opensearch object."""
        return search_data.credentials_to_es(settings.get_writer_creds())

    def delete_all_docs(self):
        """Delete all documents in opensearch (under azul.partition), leave indices untouched."""
        self.flush()
        self.es_admin.delete_by_query(
            index=self.patterns,
            body={"query": {"match_all": {}}},
            ignore=404,
            allow_no_indices=True,
        )
        self.flush()

    def delete_indexes(self):
        """Delete all indices and templates beginning with azul.partition."""
        self.es_admin.indices.delete(index=self.patterns, ignore=[404])
        self.es_admin.indices.delete_template(name=self.index_base, ignore=[404])
        self.es_admin.indices.delete_alias(index=self.index_opened, name="*", ignore=[404])
        self.es_admin.indices.delete_alias(index=self.index_closed, name="*", ignore=[404])

    @property
    def writer(self):
        """Get writer context."""
        return self.get_ctx(settings.get_writer_creds())

    def get_ctx(self, creds: dict) -> context.Context:
        """Create context using creds."""
        user_info = UserInfo(username=creds["unique"], unique_id=creds["unique"])
        sd = search_data.SearchData(credentials=creds, security_exclude=[], security_include=[])
        return self.base.copy_with(user_info=user_info, sd=sd)

    @property
    @functools.lru_cache()
    def base(self):
        return context.get_general_context()

    def setup(self, *, delete_existing=False):
        """Initialise opensearch."""
        if delete_existing:
            self.delete_indexes()
        self.writer.man.initialise(self.writer.sd, force=False)
