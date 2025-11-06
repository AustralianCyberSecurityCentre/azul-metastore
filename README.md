# Azul Metastore

Azul Metastore enables storage and retrieval of binary files and plugin execution results

- Store plugin results + info via ingestors
- Deletion of old data via age-off
- Retry of failed execution
- Manual data deletion
- Expose functionality via restapi using `azul-restapi-server`

Supplies Rest API endpoints for:

- binary submission (malpz and cart support)
- download of files (selectable neutering format)
- download of other plugin artifacts, reports, etc.
- query whether content (still) exists for binary
- Re-enqueing processing
- metadata interaction / search

## Usage

Most functionality is available via the command line script.

```bash
$ azul-metastore --help
Usage: azul-metastore [OPTIONS] COMMAND [ARGS]...

  Entrypoint to the program.

Options:
  --help  Show this message and exit.

Commands:
  age-off                  Delete expired indices.
  force-update-templates   Force opensearch templates to be added.
  ingest-binary            Ingest binary events from dispatcher.
  ingest-plugin            Ingest plugin events from dispatcher.
  ingest-status            Ingest status events from dispatcher.
  process-lost-tasks       Retry failed processing tasks from dispatcher.
  purge                    Purge metadata and data.
  apply-opensearch-config  Create roles in Opensearch that are required by Azul to function.
```

## Commands in depth

### age-off

Deletes the expired indices out of Opensearch, based on the source configuration.
This is useful to minimise the amount of data in Opensearch and delete data that is no longer required.

### force-update-templates

Updates the Opensearch templates for the current Opensearch indices.
Useful when a significant change has been made to the Opensearch model and you need to increment the index prefix.
This command will allow you to create the new templates to be used on the new indices.

### ingest-binary

Run as a pod in Azul and queries kafka through dispatcher for binary topics that have new data.
That data is then indexed and then transformed into Opensearch documents where it is then inserted into Opensearch.

### ingest-plugin

Same as binary ingestor but for plugin events.

### ingest-status

Same as binary ingestor but for status events.

### process-lost-tasks

Run as a pod in Azul and looks for events that have dequeued events but have no associated completion event.
When these events are found a message is sent to dispatcher to retry this event.

### purge

Removes all metadata and binary data about a particular hash from Azul.
It does this by deleting all the data out of S3 and Opensearch through dispatcher and metastore.

### apply-opensearch-config

Used to create the roles in Opensearch associated with the current security configuration and the necessary default roles.
To modify the roles this command creates update the security labels.

There is also a restapi component that can only be used via `azul-restapi-server` project.

## Configuration

Controlled through environment variables. See `azul_metastore/settings.py` for more info.

## Library usage

The Azul team do not recommend using the metastore as a library, as there is no guarantee 
of the stability of any public functions.

## Testing

### Running Unit Tests

Run unit tests via `pytest tests/unit`

### Running Integration tests

To setup a local instance of OpenSearch please look at `demo-cluster/readme.md`.

Run all tests via `pytest tests`.

## Project Structure

### common/

classes and utilities shared between other parts of the project

### encoders/

handle conversion between metastore searchable format and dispatcher message format

### query/

handle the querying of data from opensearch.

### restapi/

expose azul-metastore functions via rest api

### scripts/

Assorted scripts to assist different kinds of development. Not intended for use in production systems.

## Running Restapi locally

To run metastore's restapi locally you should install azul-restapi-server and a development version of metastore.

Refer to azul-restapi-server on how to startup the server locally.
