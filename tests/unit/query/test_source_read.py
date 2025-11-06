import json
import os

from azul_metastore import settings
from tests.support import unit_test


class TestSourceRead(unit_test.BaseUnitTestCase):
    def test_check_source_exists(self):
        os.environ["metastore_sources"] = json.dumps(
            {
                "source1": {
                    "elastic": {"number_of_shards": 2, "number_of_replicas": 0},
                    "expire_events_after": "1 day",
                    "partition_unit": "day",
                    "refs_required": ["another ref too long for ok", "data item 1", "item2", "ref"],
                },
                "source2": {
                    "refs_required": ["another ref too long for ok", "data item 1", "item2", "ref", "ref56"],
                },
                "source3": {
                    "elastic": {"number_of_shards": 2, "number_of_replicas": 0},
                    "refs_required": ["r1", "r2", "r3", "r4"],
                    "refs_optional": ["r5", "r6"],
                },
            }
        )
        self.assertTrue(settings.check_source_exists("source1"))
        self.assertTrue(settings.check_source_exists("source2"))
        self.assertTrue(settings.check_source_exists("source3"))
        self.assertFalse(settings.check_source_exists("source4"))

    def test_check_source_references(self):
        os.environ["metastore_sources"] = json.dumps(
            {
                "source1": {
                    "elastic": {"number_of_shards": 2, "number_of_replicas": 0},
                    "expire_events_after": "1 day",
                    "partition_unit": "day",
                    "references": [
                        {"name": "another ref too long for ok", "required": True, "description": ""},
                        {"name": "data item 1", "required": True, "description": ""},
                        {"name": "item2", "required": True, "description": ""},
                        {"name": "ref", "required": True, "description": ""},
                    ],
                },
                "source2": {
                    "references": [
                        {"name": "another ref too long for ok", "required": True, "description": ""},
                        {"name": "data item 1", "required": True, "description": ""},
                        {"name": "item2", "required": True, "description": ""},
                        {"name": "ref", "required": True, "description": ""},
                        {"name": "ref56", "required": True, "description": ""},
                    ],
                },
                "source3": {
                    "elastic": {"number_of_shards": 2, "number_of_replicas": 0},
                    "references": [
                        {"name": "r1", "required": True, "description": ""},
                        {"name": "r2", "required": True, "description": ""},
                        {"name": "r3", "required": True, "description": ""},
                        {"name": "r4", "required": True, "description": ""},
                        {"name": "r5", "required": False, "description": ""},
                        {"name": "r6", "required": False, "description": ""},
                    ],
                },
            }
        )
        settings.check_source_references("source3", {"r1": 1, "r2": 2, "r3": 3, "r4": 4})
        settings.check_source_references("source3", {"r1": 1, "r2": 2, "r3": 3, "r4": 4, "r5": 5, "r6": 6})
        self.assertRaises(
            settings.BadSourceRefsException,
            settings.check_source_references,
            *("source3", {"r1": 1, "r2": 2, "r3": 3}),
        )
        self.assertRaises(
            settings.BadSourceRefsException,
            settings.check_source_references,
            *("source3", {}),
        )
        self.assertRaises(
            settings.BadSourceRefsException,
            settings.check_source_references,
            *("source3", {"r1": 1, "r2": 2, "r3": 3, "r4": 4, "gtrhtr": 9}),
        )
