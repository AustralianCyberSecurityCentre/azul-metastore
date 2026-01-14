"""Provide mapping for a link on a source path."""

map_node = {
    "author": {
        "properties": {
            "security": {"type": "keyword"},
            "category": {"type": "keyword"},
            "name": {"type": "keyword"},
            "version": {"type": "keyword"},
        },
        "type": "object",
    },
    "sha256": {"eager_global_ordinals": True, "type": "keyword"},
    "action": {"type": "keyword"},
    "timestamp": {"type": "date"},
    "file_format": {"type": "keyword"},
    "size": {"type": "unsigned_long"},  # support large files via unsigned_long
    "filename": {"type": "keyword"},
    "language": {"type": "keyword"},
    "relationship": {
        "properties": {
            # adding extra properties here will not require reindexing all data
            # however only new documents will have the property available for search
            "action": {"type": "keyword"},
            "label": {"type": "keyword"},
        },
        "dynamic": "false",  # ignore extra properties here
        "type": "object",
    },
    "encoded": {
        # derived information that can be easily removed on read
        "properties": {
            # entity + author + event + stream
            "sha256_author_action": {"type": "keyword"},
        },
        "type": "object",
    },
}
