"""CLI entrypoint to metastore."""

import logging
import os
import time
import traceback
from enum import IntEnum

import click
from prometheus_client import start_http_server

from azul_metastore import context, entry_purge, ingestor, settings
from azul_metastore.common import manager, search_data
from azul_metastore.opensearch_config import (
    get_opensearch_cli_commands,
    write_config_to_opensearch,
)
from azul_metastore.query import age_off as _age_off

logger = logging.getLogger(__name__)


def start_prometheus_server():
    """Start the prometheus server if the prometheus port is set."""
    port = settings.Metastore().prometheus_port
    if port:
        logger.info(f"starting prometheus metrics server started on port {port}")
        start_http_server(port)


@click.group()
def cli():
    """Entrypoint to the program."""
    pass


@cli.command()
def force_update_templates():
    """Force opensearch templates to be added.

    Note this can only add properties, not remove or modify existing properties.
    """
    man = manager.Manager()
    man.initialise(sd=search_data.get_writer_search_data(), force=True)


@cli.command()
def ingest_plugin():
    """Ingest plugin events from dispatcher."""
    start_prometheus_server()
    ctx = context.get_writer_context()
    ing = ingestor.PluginIngestor(ctx)
    ing.main()


@cli.command()
def ingest_binary():
    """Ingest binary events from dispatcher."""
    start_prometheus_server()
    ctx = context.get_writer_context()
    ing = ingestor.BinaryIngestor(ctx)
    ing.main()


@cli.command()
def ingest_status():
    """Ingest status events from dispatcher."""
    start_prometheus_server()
    ctx = context.get_writer_context()
    ing = ingestor.StatusIngestor(ctx)
    ing.main()


class AuthOptions(IntEnum):
    """Authentication options for logging to Opensearch and creating roles."""

    user_and_password = 1
    jwt = 2
    oauth_token = 3


@cli.command()
@click.option(
    "--print-only",
    is_flag=True,
    default=False,
    help="Print the metastore API commands to create the roles rather than using the API.",
)
@click.option(
    "--rolesmapping",
    is_flag=True,
    default=False,
    help="Also generate a role mapping for external roles named using 'unsafe' security names. "
    + "You likely want to create a custom mapping instead.",
)
@click.option(
    "--no-input",
    is_flag=True,
    default=False,
    help="Continue role creation without prompting for confirmation (non-interactive use).",
)
def apply_opensearch_config(print_only: bool, rolesmapping: bool, no_input: bool = False):
    """Apply the current security configuration to Opensearch."""
    if rolesmapping:
        click.echo("Additionally creating role mappings.")

    if print_only:
        click.echo("Generating Opensearch Interactive API commands to create security resources.")
        commands = get_opensearch_cli_commands(rolesmapping)
        result = (
            "------------------------\nThe Opensearch commands to create the necessary roles are as follows:\n\n"
            + "\n".join(commands)
        )
        click.echo(result)
    # Check for env vars and set selected if present
    env_username = os.environ.get("METASTORE_OPENSEARCH_ADMIN_USERNAME")
    env_password = os.environ.get("METASTORE_OPENSEARCH_ADMIN_PASSWORD")
    if no_input and not (env_username and env_password):
        raise ValueError(
            "When using --no-input, both METASTORE_OPENSEARCH_ADMIN_USERNAME and "
            "METASTORE_OPENSEARCH_ADMIN_PASSWORD must be set."
        )

    elif no_input or click.confirm(
        text="Will create the security resources in Opensearch.\nContinue?",
        default=False,
        abort=True,
    ):
        options = [f"{v} - {k}" for k, v in AuthOptions._member_map_.items()]
        if env_username and env_password:
            selected = AuthOptions.user_and_password
        else:
            selected = None

        while not selected:
            try:

                int_val = click.prompt(
                    text="For auth please select" + f" from the following options (enter the number) {options}",
                    type=int,
                )
                selected = AuthOptions(int_val)
            except Exception:
                click.echo(
                    f"Provided input option was invalid '{int_val}' is not a"
                    + f" valid option must be one of {", ".join([str(x.value) for x in AuthOptions])}."
                )

        credentials = None
        if selected == AuthOptions.user_and_password:
            username = env_username or click.prompt(text="Please provide the username for Opensearch: ")
            password = env_password or click.prompt(text="Please provide the password for Opensearch: ")
            credentials = {"unique": username, "format": "basic", "username": username, "password": password}
        elif selected == AuthOptions.jwt:
            token = click.prompt(text="Please provide the JWT for Opensearch: ")
            credentials = {"unique": "local-user-jwt", "format": "jwt", "token": token}
        elif selected == AuthOptions.oauth_token:
            token = click.prompt(text="Please provide the OAuth token for Opensearch: ")
            credentials = {"unique": "local-user-oauth", "format": "jwt", "token": token}
        else:
            click.echo("Error. Must provide credentials.")
            return 7
        credentials = search_data.SearchData(credentials, security_exclude=[], security_include=[])
        try:
            write_config_to_opensearch(credentials, rolesmapping)
        except Exception:
            click.echo(f"Failed to update and validate roles with exception traceback:\n{traceback.format_exc()}")
            return
        click.echo("Successfully created and validated all roles.")


@cli.command()
@click.option("--loop/--no-loop", default=True)
def age_off(loop):
    """Delete expired indices."""
    start_prometheus_server()
    logger.info("started age off worker")
    while True:
        # find old documents and delete
        _age_off.do_age_off()
        if not loop:
            break

        logger.info("wait for an hour")
        # wait for an hour
        time.sleep(60 * 60)


cli.add_command(entry_purge.cli)


@cli.result_callback()
def _finished(result, **kwargs):
    logger.info("command finished")


if __name__ == "__main__":
    cli()
