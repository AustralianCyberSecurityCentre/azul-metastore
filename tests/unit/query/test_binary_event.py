from azul_metastore.query.binary2 import binary_event
from tests.support import unit_test


class TestBinaryRead(unit_test.BaseUnitTestCase):
    def test_opensearch_mapping(self):
        """Tests that OpenSearch mappings can be correctly converted to a flat lookup dict."""
        # Test a leaf node
        converted = binary_event._convert_opensearch_mapping_to_flat({"test": {"type": "float"}})

        self.assertEqual(converted, {"test": "float"})

        # Test an object node
        converted = binary_event._convert_opensearch_mapping_to_flat(
            {"test": {"type": "object", "properties": {"child": {"type": "float"}}}}
        )

        self.assertEqual(converted, {"test.child": "float"})

        # Test a security node
        converted = binary_event._convert_opensearch_mapping_to_flat(
            {
                "test": {
                    "type": "object",
                    "properties": {
                        "security": {
                            "type": "object",
                            "properties": {"inclusive": {"type": "keyword"}, "exclusive": {"type": "keyword"}},
                        }
                    },
                }
            }
        )

        self.assertEqual(converted, {})

        # Test a mix of security and non-security nodes
        converted = binary_event._convert_opensearch_mapping_to_flat(
            {
                "test": {
                    "type": "object",
                    "properties": {
                        "security": {
                            "type": "object",
                            "properties": {"inclusive": {"type": "keyword"}, "exclusive": {"type": "keyword"}},
                        },
                        "child": {"type": "float"},
                    },
                }
            }
        )

        self.assertEqual(converted, {"test.child": "float"})

        # Test mixing multi levels of nodes
        converted = binary_event._convert_opensearch_mapping_to_flat(
            {
                "test": {
                    "type": "object",
                    "properties": {
                        "child": {"type": "float"},
                        "child2": {
                            "type": "object",
                            "properties": {
                                "subchild": {"type": "object", "properties": {"subsubchild": {"type": "float"}}}
                            },
                        },
                    },
                },
                "test2": {"type": "float"},
            }
        )

        self.assertEqual(
            converted, {"test.child": "float", "test.child2.subchild.subsubchild": "float", "test2": "float"}
        )

        # Test error handling for invalid types
        with self.assertRaises(ValueError):
            binary_event._convert_opensearch_mapping_to_flat({"something_cool": {"value": "123"}})
