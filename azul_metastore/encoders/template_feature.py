"""Provides a mapping for a feature value."""

# by default, opensearch throws an error if keywords are much longer than this.
# ignore_above means that opensearch will instead refuse to index strings with excessive length.
MAX_VALUE_LENGTH = 4000

map_feature = {
    "name": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH, "doc_values": False},
    "value": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH, "doc_values": False},
    "type": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH, "doc_values": False},
    # extras
    "label": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH, "doc_values": False},
    "offset": {"type": "long", "doc_values": False},
    "size": {"type": "long", "doc_values": False},
    "enriched": {
        "properties": {
            # value fields based off of value type
            "integer": {"type": "long", "doc_values": False},
            "float": {"type": "double", "doc_values": False},
            # for specialised search
            "datetime": {"type": "date", "doc_values": False},
            "binary_string": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH, "doc_values": False},
            "scheme": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH, "doc_values": False},
            "netloc": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH, "doc_values": False},
            "filepath": {
                "fields": {
                    "tree": {"analyzer": "path", "type": "text", "norms": False, "doc_values": False},
                    "tree_reversed": {
                        "analyzer": "path_reversed",
                        "type": "text",
                        "norms": False,
                        "doc_values": False,
                    },
                },
                "type": "keyword",
                "ignore_above": MAX_VALUE_LENGTH,
                "doc_values": False,
            },
            "params": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH, "doc_values": False},
            "query": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH, "doc_values": False},
            "fragment": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH, "doc_values": False},
            "username": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH, "doc_values": False},
            "password": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH, "doc_values": False},
            "hostname": {"type": "keyword", "ignore_above": MAX_VALUE_LENGTH, "doc_values": False},
            "port": {"type": "integer", "doc_values": False},
            "ip": {"type": "ip", "doc_values": False},
        },
        "type": "object",
    },
    "encoded": {
        "properties": {
            # find overlapping features
            "location": {"type": "double_range", "doc_values": False},
        },
        "type": "object",
    },
}
