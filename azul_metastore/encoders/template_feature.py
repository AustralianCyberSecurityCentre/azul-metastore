"""Provides a mapping for a feature value."""

# by default, opensearch throws an error if keywords are much longer than this.
# ignore_above means that opensearch will instead refuse to index strings with excessive length.
MAX_VALUE_LENGTH = 8000

map_feature = {
    "name": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH},
    "value": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH},
    "type": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH},
    # extras
    "label": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH},
    "offset": {"type": "long"},
    "size": {"type": "long"},
    "enriched": {
        "properties": {
            # value fields based off of value type
            "integer": {"type": "long"},
            "float": {"type": "double"},
            # for specialised search
            "datetime": {"type": "date"},
            "binary_string": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH},
            "scheme": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH},
            "netloc": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH},
            "filepath": {
                "fields": {
                    "tree": {"analyzer": "path", "type": "text"},
                    "tree_reversed": {"analyzer": "path_reversed", "type": "text"},
                },
                "type": "keyword",
                "ignore_above": MAX_VALUE_LENGTH,
            },
            "params": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH},
            "query": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH},
            "fragment": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH},
            "username": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH},
            "password": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH},
            "hostname": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH},
            "port": {"type": "integer"},
            "ip": {"type": "ip"},
        },
        "type": "object",
    },
    "encoded": {
        "properties": {
            # find overlapping features
            "location": {"type": "double_range"},
        },
        "type": "object",
    },
}
