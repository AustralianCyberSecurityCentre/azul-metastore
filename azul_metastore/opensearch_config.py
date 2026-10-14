"""Run commands against Opensearch to create the roles needed by Azul."""

import copy
import json
import logging

import azul_security.settings
from opensearchpy.exceptions import NotFoundError

from azul_metastore.common import search_data, utils
from azul_metastore.settings import Metastore
from azul_metastore.settings import get as get_metastore_settings

logger = logging.getLogger(__name__)


def _get_role_creation_bodies(roles: list[str]) -> dict[str, dict]:
    """Get the bodies for roles that need to be created."""
    roles = roles.copy()
    role_bodies = dict()
    # --------------------------------------- azul-access/ Read setting
    opensearch_settings = get_metastore_settings()
    if opensearch_settings.opensearch_read_role in roles:
        roles.remove(opensearch_settings.opensearch_read_role)
        # Build dls query in parts to make it clear what sections are for and make markings section optional.
        # Start of outer section of dls query
        dls_query = """{"bool": {"filter": ["""
        # inclusive query
        dls_query += """{"terms": {"encoded_security.inclusive": [${user.securityRoles}]}}"""
        # exclusive query
        dls_query += """,{"terms_set": {"encoded_security.exclusive": {"terms": [${user.securityRoles}], "minimum_should_match_field": "encoded_security.num_exclusive"}}}"""  # noqa: E501
        # markings query (only if enforced)
        security_settings = azul_security.settings.Settings()
        unsafe_enforceable_markings = utils.azsec().unsafe_to_safe(list(security_settings.enforceable_markings))
        if len(unsafe_enforceable_markings) > 0:
            dls_query += (
                """,{"bool":{"should":[{"bool":{"must_not":{"terms":{"encoded_security.markings":["""
                + ",".join(f'"{marking}"' for marking in unsafe_enforceable_markings)
                + """]}}}},{"bool":{"must":{"terms":{"encoded_security.markings":[${user.securityRoles}]}}}}],"minimum_should_match":1}}"""  # noqa: E501
            )  # noqa: E501
        # end of dls query
        dls_query += """]}}"""

        role_bodies[opensearch_settings.opensearch_read_role] = {
            "cluster_permissions": [
                "indices:data/read/*",
                "kibana_user",  # Reserved role
            ],
            "index_permissions": [
                {
                    "index_patterns": ["*"],
                    "allowed_actions": ["indices:admin/resolve/index"],
                    "fls": [],
                    "masked_fields": [],
                },
                {
                    "index_patterns": ["azul.*"],
                    "allowed_actions": ["indices:admin/get", "indices:admin/analyze"],
                    "fls": [],
                    "masked_fields": [],
                },
                {
                    "index_patterns": ["azul.x.*"],
                    "dls": dls_query,
                    "allowed_actions": ["read"],
                    "fls": [],
                    "masked_fields": [],
                },
                {
                    "index_patterns": ["azul.o.*"],
                    "allowed_actions": ["read"],
                    "fls": [],
                    "masked_fields": [],
                },
            ],
        }
    # --------------------------------------- opensearch_settings.opensearch_write_role
    if opensearch_settings.opensearch_write_role in roles:
        roles.remove(opensearch_settings.opensearch_write_role)
        role_bodies[opensearch_settings.opensearch_write_role] = {
            "cluster_permissions": [
                "cluster_monitor",
                "cluster:admin/script/*",
                "indices:admin/index_template/*",
                "indices:admin/mapping/*",
                "indices:admin/template/*",
                "indices:data/read/*",
                "indices:data/write/bulk",
                "kibana_user",
            ],
            "index_permissions": [
                {
                    "index_patterns": ["azul.*"],
                    "fls": [],
                    "masked_fields": [],
                    "allowed_actions": [
                        "unlimited",
                    ],
                }
            ],
        }
    # All other roles (s-any is included)
    other_roles = {
        "cluster_permissions": [],
        "index_permissions": [],
    }
    for r in roles:
        role_bodies[r] = other_roles.copy()
    return role_bodies


def required_opensearch_roles(
    metastore_setting: Metastore, security_settings: azul_security.settings.Settings
) -> list:
    """Map unsafe to safe and add builtin opensearch specific mappings."""
    copy_unsafe_to_safe = security_settings._unsafe_to_safe.copy()
    result = list(copy_unsafe_to_safe.values())
    result.append("s-any")
    result.append(metastore_setting.opensearch_read_role)
    result.append(metastore_setting.opensearch_write_role)
    # fillers to prevent Opensearch prevent SG 900D errors
    result.append("azul-fill1")
    result.append("azul-fill2")
    result.append("azul-fill3")
    result.append("azul-fill4")
    result.append("azul-fill5")

    return result


def generate_security_resources() -> tuple[list[tuple[str, dict]], list[tuple[str, dict]]]:
    """Generate roles and role mappings required."""
    security_settings = azul_security.settings.Settings()
    metastore_settings = get_metastore_settings()

    ret_roles = []
    ret_role_mappings = []

    roles = sorted(required_opensearch_roles(metastore_settings, security_settings))
    role_body = _get_role_creation_bodies(roles)
    for role_name, body in role_body.items():
        ret_roles.append((role_name, body))

    # create/update role mappings for standard security options
    role_mappings = copy.copy(security_settings.safe_to_unsafe)
    # add standard accesses (assumes external group configured in opensearch_settings.opensearch_read_role)
    role_mappings[metastore_settings.opensearch_read_role] = metastore_settings.opensearch_read_role
    role_mappings["s-any"] = metastore_settings.opensearch_read_role
    role_mappings[metastore_settings.opensearch_write_role] = metastore_settings.opensearch_write_role
    role_mappings["azul-fill1"] = metastore_settings.opensearch_read_role
    role_mappings["azul-fill2"] = metastore_settings.opensearch_read_role
    role_mappings["azul-fill3"] = metastore_settings.opensearch_read_role
    role_mappings["azul-fill4"] = metastore_settings.opensearch_read_role
    role_mappings["azul-fill5"] = metastore_settings.opensearch_read_role

    for safe, unsafe in role_mappings.items():
        if safe not in roles:
            continue
        ret_role_mappings.append((safe, {"backend_roles": [unsafe], "description": f"Maps {unsafe} to this role"}))

    ret_roles.sort(key=lambda x: x[0])
    ret_role_mappings.sort(key=lambda x: x[0])
    return ret_roles, ret_role_mappings


def get_opensearch_cli_commands(rolesmapping: bool) -> list[str]:
    """Get a list of Opensearch commands to create the necessary roles in Opensearch."""
    roles, role_mappings = generate_security_resources()

    commands: list[str] = []
    for role_name, body in roles:
        commands.append(
            f"PUT _plugins/_security/api/roles/{role_name}\n" + str(json.dumps(body, sort_keys=True, indent=4))
        )

    if rolesmapping:
        for role_name, body in role_mappings:
            commands.append(
                f"PUT _plugins/_security/api/rolesmapping/{role_name}\n"
                + str(json.dumps(body, sort_keys=True, indent=4))
            )

    return commands


def write_config_to_opensearch(sd: search_data.SearchData, rolesmapping: bool):
    """Creates the provided roles in Opensearch using the Opensearch API."""
    roles, role_mappings = generate_security_resources()

    for role_name, body in roles:
        try:
            # Check if the role exists
            sd.es().security.get_role(role=role_name)
            logger.info(f"Role '{role_name}' already exists. Skipping creation.")
        except NotFoundError:
            # Role does not exist, create it
            sd.es().security.create_role(role=role_name, body=body)
            logger.info(f"Created role '{role_name}'.")
        except Exception as e:
            logger.error(f"Error checking/creating role '{role_name}': {e}")

    if rolesmapping:
        for role_mapping, body in role_mappings:
            try:
                # Check if the role mapping exists
                sd.es().security.get_role_mapping(role=role_mapping)
                logger.info(f"Role mapping '{role_mapping}' already exists. Skipping creation.")
            except NotFoundError:
                # Role mapping does not exist, create it
                sd.es().security.create_role_mapping(role=role_mapping, body=body)
                logger.info(f"Created role mapping '{role_mapping}' for '{body['backend_roles'][0]}'.")
            except Exception as e:
                logger.error(f"Error checking/creating role mapping '{role_mapping}': {e}")
