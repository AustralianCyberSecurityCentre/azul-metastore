"""CLI interface to perform data purge actions."""

import logging

import black
import click
from azul_bedrock.models_restapi import purge as bedr_purge

from azul_metastore.query import purge as qpurge

logger = logging.getLogger(__name__)


def _render_simulation(sum: bedr_purge.PurgeSimulation):
    # we use black to format the string for convenience
    click.echo(f"simulation results:\n{black.format_str(repr(sum), mode=black.Mode())}")


@click.group(name="purge")
def cli():
    """Purge metadata and data."""
    pass


@cli.command()
@click.argument("track_source_references")
@click.option("--timestamp", default=None, help="Submission timestamp.")
@click.option("--purge", is_flag=True, default=False, help="Purge data instead of simulating.")
def submission(track_source_references: str, timestamp: str | None, purge: bool):
    """Purge binary events associated with a binary submission to a source."""
    purger = qpurge.Purger()
    ret = purger.purge_submission(
        track_source_references=track_source_references,
        timestamp=timestamp,
        purge=purge,
    )
    _render_simulation(ret)


@cli.command()
@click.argument("track_author")
@click.option("--purge", is_flag=True, default=False, help="Purge data, do not simulate.")
def author(track_author: str, purge: bool):
    """Purge binary events associated with an author. Use track_authors value as the argument, not author.name."""
    purger = qpurge.Purger()
    ret = purger.purge_author(
        track_author=track_author,
        purge=purge,
    )
    _render_simulation(ret)


@cli.command()
@click.argument("track_link")
@click.option("--purge", is_flag=True, default=False, help="Purge data, do not simulate.")
def link(track_link: str, purge: bool):
    """Purge manually inserted links between two binaries."""
    purger = qpurge.Purger()
    ret = purger.purge_link(
        track_link=track_link,
        purge=purge,
    )
    _render_simulation(ret)
