# Metadata Template Upgrade

Occasionally it is necessary to alter the mapping template used by opensearch for binary/plugin/status documents.

If existing properties have been altered, a reingest will be required.
The ingestors should be pointed to a new 'partition' in settings.
When run, they will create the new required indices with the correct templating.
