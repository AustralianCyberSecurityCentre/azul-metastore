The metastore encoders can handle:

- URLs extracted from files
- Text blocks extracted from files
- entropy information about how the file is structured
- file names
- and more

All metadata is stored as a series of 'result' documents. See below for more information.

The metastore provides functionality for a number of different queries over this stored metadata.

- Find ancestors and descendants of an entity
- Find entities produced by a specific plugin
- Retrieve a merged view of all documents for an entity
- Search all entities for a specific url, text, feature, value, etc.
- Find similar entities to an existing entity
- Etc.

## Result Document

A result document is a unique set of metadata produced by a plugin when it
runs over a unique entity descending from a unique source via a unique path.

- plugin - A processor that produces metadata from inserted data. Can also produce descendant entities.
  - e.g. a config extractor for a family of malware, an entropy calculator, archive extractor.
- entity - A unique piece of data such as a pdf, exe.
  May also be a reference to a unique piece of data that Azul may not actually contain. e.g. files found in virustotal, files found from threat reporting.
- source - A unique label indicating where an entity came from. e.g. network_internal_01, virustotal, samples.
- source entity - The entity directly inserted into a source. Any entity with an ancestor cannot be a source entity.

The result document is stored in opensearch via the metastore ingestor loop, which continually polls
the Azul dispatcher for more results. It must be transformed into an opensearch compatible structure before
it is stored.

Result documents are explicitly not merged together before storing in opensearch because:

- would need to avoid duplication of data (if same plugin runs again) - slow
  - you would need a bunch more unique ids for each piece of data
  - need to iterate over each piece of data that already exists for the entity
  - requires constantly running opensearch painless scripts
  - painless scripts are painful
- would makes deletions very difficult
  - would need another painless script to read each piece of data for the entity to check right document was being deleted.
