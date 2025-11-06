# Data Retention

Events in the metastore build up over time. It may be desirable to remove old events to free up space.

## Status Events

By default the last 14 days worth of status events are kept. This is specified via `status_expire_events`.

Valid `status_expire_events` are in the form `<number> 'years'|'months'|'weeks'|'days'` e.g 4 months.

## Source Events

Each source is able to specify how to split indices and how often to delete documents independently.

    * `partition_unit` - specifies the time-bucket index used for events (day|week|month|year)
    * `expire_events` - specifies how old documents can be before deleting them. Form is same as `status_expire_events`.

For example source='mycollect' has partition_unit='month' and expire_events='6 weeks' means 'split the indices up by month and only keep the last 6 weeks of data.

Valid `partition_unit` settings are currently 'year', 'month', 'week', 'day'.

## Deletion

Old events are deleted by query when they are older than the expire_events threshold for their source.
The whole index is deleted when every document in the index exceeds the expire_events threshold.

### Index Evaluation

For all indices covered by these retention settings, the newest documents in the indices are found.

Timestamp used for this is as measured by `source.timestamp` field.

If this newest document is older than `oldest_to_keep`, the entire index is deleted.

Then any remaining documents are deleted by query if they exceed the expire_events threshold.

```text
azul.o.dev01.binary.mycollect.2023-01 # delete
azul.o.dev01.binary.mycollect.2023-02 # delete
azul.o.dev01.binary.mycollect.2023-03 # deleting old events but not the index yet.
azul.o.dev01.binary.mycollect.2023-04
azul.o.dev01.binary.mycollect.2023-05
azul.o.dev01.binary.mycollect.2023-06
```

Expect the number of indices for a given window to be `retention_keep_last + 1`, when sufficient time has passed. Allow for this when estimating storage size required.
