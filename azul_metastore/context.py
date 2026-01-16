"""Query context class for querying opensearch database."""

from dataclasses import dataclass

import cachetools
from azul_bedrock import dispatcher as b_dispatcher
from azul_bedrock import models_auth
from azul_bedrock.exceptions import ApiException
from azul_bedrock.models_restapi.basic import UserAccess
from azul_security import admin
from azul_security import security as sec
from azul_security.exceptions import SecurityException
from starlette.status import HTTP_422_UNPROCESSABLE_CONTENT

from azul_metastore import settings
from azul_metastore.common import manager, memcache, opensearch, search_data, wrapper


class NoWriteException(Exception):
    """User does not have permission to write documents."""

    pass


@cachetools.cached(cache=memcache.get_ttl_cache("useraccess"), key=lambda x, _: x.unique())
def _get_user_access(sd: search_data.SearchData, azsec: sec.Security) -> UserAccess:
    """Return the users list of security labels they are able to view in the system.

    This information is returned with every restapi query.
    It is also used to determine access permissions of opensearch-cached data like counts of binaries.
    """
    ret = UserAccess()
    s = settings.get()
    if s.no_security_plugin_compatibility:
        # security is disabled so all access granted
        ret.security_enabled = False
        security_labels = sorted(azsec.get_labels_allowed())
    else:
        ret.security_enabled = True
        ret.account_info = opensearch.get_user_account(sd.access())
        
        # read internal roles as security labels translated by opensearch
        # i.e. 'ACCESS2' on JWT is translated to 's-rel-apple' role by opensearch
        ret.roles = sorted(set(ret.account_info["roles"]))
        # filter out generic opensearch roles
        generic_roles = ["own_index"]
        ret.roles = [x for x in ret.roles if x not in generic_roles]

        # privileged role so all access granted (assumed but might not be correct if bad metastore config)
        if s.admin_role in ret.roles or s.opensearch_username == ret.account_info["user_name"]:
            ret.privileged = True
            security_labels = sorted(azsec.get_labels_allowed())
        else:
            # convert roles to valid labels (s-rel-apple -> REL:APPLE)
            # Get all markings that aren't enforced.
            open_markings = set(azsec.get_labels_markings()) - set(
                azsec.get_enforceable_markings(list(azsec.get_labels_markings()))
            )
            security_labels = sorted(azsec.safe_to_unsafe(ret.roles, drop_mismatch=True) + list(open_markings))
<<<<<<< Updated upstream

    # ret.security = azsec.summarise_user_access(labels=security_labels, denylist=sd.security_exclude)
    ret.security = azsec.summarise_user_access(
        labels=security_labels,
        denylist=sd.security_exclude,
        includelist=sd.security_include,
        sec_filter=sd.security_filter,
    )
    print("Summarised access ", ret)
=======
    ret.security = azsec.summarise_user_access(labels=security_labels, denylist=sd.security_exclude, includelist=sd.security_include)
    print("THIS IS SECURITY BEFORE MAXIG ", ret)
>>>>>>> Stashed changes
    return ret


@dataclass
class Context:
    """Helper context for interacting with metastore. used to maintain state."""

    azsec: sec.Security
    man: manager.Manager

    user_info: models_auth.UserInfo = None
    sd: search_data.SearchData = None
    dispatcher: b_dispatcher.DispatcherAPI = None

    def refresh(self):
        """Refresh the searchable docs for all indices in partition."""
        self.sd.es().indices.refresh(index=".".join(["azul", "*"]))

    def copy_with(self, user_info: models_auth.UserInfo, sd: search_data.SearchData, **kwargs):
        """Copy the object using a new search data arg."""
        return self.__class__(
            azsec=self.azsec,
            man=self.man,
            user_info=user_info,
            sd=sd,
            dispatcher=self.dispatcher,
            **kwargs,
        )

    def clear_state(self):
        """Clear any state on the context."""
        # search data should not persist between queries
        self.sd.clear_state()

    def is_admin(self) -> bool:
        """Return true if the user is admin and false if they are not."""
        return admin.is_user_admin(self.user_info)

    def get_user_access(self) -> UserAccess:
        """Return user access as reported by opensearch."""
        return _get_user_access(self.sd, self.azsec)

    def get_user_current_security(self) -> str:
        """Return security string for current users access."""
<<<<<<< Updated upstream
        print("USER ACCESS ", self.get_user_access())
=======
        #return self.get_user_access().security.max_access
>>>>>>> Stashed changes
        return self.get_user_access().security.max_access

    def get_user_security_unique(self) -> str:
        """Return md5 of security markings."""
        return self.get_user_access().security.unique

    def validate_user_security(self, security: str) -> str:
        """Validates a user-supplied security string, raising a HTTP exception if invalid."""
        try:
            return self.azsec.string_normalise(security)
        except SecurityException:
            raise ApiException(
                status_code=HTTP_422_UNPROCESSABLE_CONTENT,
                ref="security was not valid",
                external=f"security was not valid ({security})",
                internal="upload_bad_security",
            )


def get_general_context() -> Context:
    """Get a generic context. opensearch object must be loaded before this can be used for querying."""
    s = settings.get()

    return Context(
        azsec=sec.Security(),
        man=manager.Manager(),
        dispatcher=b_dispatcher.DispatcherAPI(
            events_url=s.dispatcher_events_url,
            data_url=s.dispatcher_streams_url,
            retry_count=3,
            timeout=15,
            author_name="metastore",
            author_version="1",
            deployment_key="",
        ),
    )


@cachetools.cached(cache=memcache.get_lru_cache("get_writer_context"))
def get_writer_context() -> Context:
    """Return the writer context."""
    s = settings.get()
    ret = get_general_context().copy_with(
        # Technically a risk that if the OIDC "sub" field is equal to opensearch_username you could be granted
        # permissions you shouldn't have as a generic user.
        user_info=models_auth.UserInfo(username=s.opensearch_username, unique_id=s.opensearch_username),
        sd=search_data.get_writer_search_data(),
    )
    try:
        ret.man.initialise(ret.sd)
    except wrapper.InitFailure as e:
        raise NoWriteException("Could not initialise metastore templates") from e
    return ret
