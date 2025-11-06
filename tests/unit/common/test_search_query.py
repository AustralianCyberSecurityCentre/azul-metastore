from typing import Literal, Optional

from azul_bedrock.models_restapi import binaries_auto_complete as bedr_bauto

from azul_metastore.common import search_query, search_query_parser
from tests.support import unit_test

# Expected case_insensitive result
_CASE_INSENSITIVE_RESULTS = {"double": False, "single": True, None: True}


class TestSearchQuery(unit_test.BaseUnitTestCase):
    def _generate_range_token(
        self, start=5, end=10, start_inclusive=True, end_inclusive=True, token_start=0, token_end=15
    ):
        """Generates an example range expression."""
        return search_query.RangeExpression(
            start=start,
            end=end,
            startInclusive=start_inclusive,
            endInclusive=end_inclusive,
            location=search_query_parser.TokenLocation(start=token_start, end=token_end),
        )

    def _generate_string_token(
        self,
        value="testvalue",
        quotes: Optional[Literal["double"] | Literal["single"]] = None,
        token_start=0,
        token_end=None,
    ):
        """Generates an example string expression."""
        if not token_end:
            token_end = token_start + len(value)
        return search_query.StringExpression(
            value=value,
            quotes=quotes,
            location=search_query_parser.TokenLocation(start=token_start, end=token_end),
        )

    def _generate_number_token(
        self,
        value=1234,
        token_start=0,
        token_end=4,
    ):
        """Generates an example numeric expression."""
        return search_query.NumberExpression(
            value=value,
            location=search_query_parser.TokenLocation(start=token_start, end=token_end),
        )

    def _assert_field_search(
        self,
        expected: dict,
        key: str = "test",
        operator: search_query_parser.FieldComparison = ":",
        field: (
            search_query_parser.StringExpression
            | search_query_parser.NumberExpression
            | search_query_parser.RangeExpression
            | None
        ) = None,
    ):
        """Asserts that the output of converting a field search token to OpenSearch syntax matches expected output."""
        self.assertEqual(
            search_query._az_field_search_to_opensearch(
                None, key, operator, field, extra_info=search_query.QueryExtraInfo()
            ),
            expected,
        )

    def test_range_conversion(self):
        """Tests range expressions being converted to OpenSearch syntax."""
        for start_inclusive in [True, False]:
            for end_inclusive in [True, False]:
                with self.subTest(start_inclusive=start_inclusive, end_inclusive=end_inclusive):
                    start_inclusive_label = "gte" if start_inclusive else "gt"
                    end_inclusive_label = "lte" if end_inclusive else "lt"
                    self._assert_field_search(
                        field=self._generate_range_token(
                            start=5, end=10, start_inclusive=start_inclusive, end_inclusive=end_inclusive
                        ),
                        expected={"range": {"test": {start_inclusive_label: 5, end_inclusive_label: 10}}},
                    )

    def test_string_conversion(self):
        """Tests string expressions being converted to OpenSearch syntax."""
        for quotes, result in _CASE_INSENSITIVE_RESULTS.items():
            with self.subTest(repr(quotes)):
                self._assert_field_search(
                    # list constrained to known values, so ignoring bad typing
                    field=self._generate_string_token(value="testvalue*", quotes=quotes),  # type: ignore
                    expected={"prefix": {"test": {"value": "testvalue", "case_insensitive": result}}},
                )

    def test_wildcard_conversion(self):
        """Tests string expressions with wildcards in OpenSearch syntax."""
        for quotes, result in _CASE_INSENSITIVE_RESULTS.items():
            with self.subTest(repr(quotes)):
                self._assert_field_search(
                    # list constrained to known values, so ignoring bad typing
                    field=self._generate_string_token(value="testvalue*", quotes=quotes),  # type: ignore
                    expected={"prefix": {"test": {"value": "testvalue", "case_insensitive": result}}},
                )

    def test_numeric_expressions(self):
        """Tests numeric expressions being converted to OpenSearch syntax."""
        for operator in [":", ":="]:
            with self.subTest(operator):
                self._assert_field_search(
                    field=self._generate_number_token(value=1234),
                    # list constrained to known values, so ignoring bad typing
                    operator=operator,  # type: ignore
                    expected={"term": {"test": 1234}},
                )

        operators = {":>=": "gte", ":>": "gt", ":<": "lt", ":<=": "lte"}

        for operator, expected in operators.items():
            with self.subTest(operator):
                self._assert_field_search(
                    field=self._generate_number_token(value=1234),
                    # list constrained to known values, so ignoring bad typing
                    operator=operator,  # type: ignore
                    expected={"range": {"test": {expected: 1234}}},
                )

    def test_field_exists(self):
        """Tests that a field can be checked for its existence."""
        self._assert_field_search(
            field=None,
            expected={"exists": {"field": "test"}},
        )

    def _generate_tag(
        self,
        key: Optional[search_query_parser.StringExpression] = None,
        operator: Optional[search_query_parser.LogicalOperator] = None,
        value: (
            search_query_parser.StringExpression
            | search_query_parser.NumberExpression
            | search_query_parser.RangeExpression
        ) = search_query.StringExpression(
            location=search_query_parser.TokenLocation(start=5, end=15), quotes=None, value="testt"
        ),
        token_start=0,
        token_end=15,
    ):
        """Generates a tag for testing."""
        return search_query.Tag(
            location=search_query_parser.TokenLocation(start=token_start, end=token_end),
            key=key,
            operator=operator,
            value=value,
        )

    def test_logical_operator(self):
        """Tests that logical operators behave correctly."""
        test_tag = search_query.Tag(
            location=search_query_parser.TokenLocation(start=0, end=15),
            key=search_query.StringExpression(
                location=search_query_parser.TokenLocation(start=0, end=4), quotes=None, value="test"
            ),
            operator=":",
            value=search_query.StringExpression(
                location=search_query_parser.TokenLocation(start=5, end=15), quotes=None, value="testt"
            ),
        )
        test_tag2 = search_query.Tag(
            location=search_query_parser.TokenLocation(start=0, end=15),
            key=search_query.StringExpression(
                location=search_query_parser.TokenLocation(start=0, end=4), quotes=None, value="test2"
            ),
            operator=":",
            value=search_query.StringExpression(
                location=search_query_parser.TokenLocation(start=5, end=15), quotes=None, value="testt2"
            ),
        )

        self.assertEqual(
            search_query.az_query_to_opensearch(
                None, search_query.LogicalOperator(operator="AND", children=[test_tag, test_tag2])
            )[0],
            {
                "bool": {
                    "filter": [
                        search_query.az_query_to_opensearch(None, test_tag)[0],
                        search_query.az_query_to_opensearch(None, test_tag2)[0],
                    ]
                }
            },
        )

        self.assertEqual(
            search_query.az_query_to_opensearch(
                None, search_query.LogicalOperator(operator="OR", children=[test_tag, test_tag2])
            )[0],
            {
                "bool": {
                    "should": [
                        search_query.az_query_to_opensearch(None, test_tag)[0],
                        search_query.az_query_to_opensearch(None, test_tag2)[0],
                    ],
                    "minimum_should_match": 1,
                }
            },
        )

        self.assertEqual(
            search_query.az_query_to_opensearch(
                None, search_query.LogicalOperator(operator="NOT", children=[test_tag])
            )[0],
            {
                "bool": {
                    "filter": [{"exists": {"field": "test"}}],
                    "must_not": [
                        search_query.az_query_to_opensearch(None, test_tag)[0],
                    ],
                }
            },
        )

    def test_tag(self):
        """Tests that tags behave correctly."""
        test_tag = search_query.Tag(
            location=search_query_parser.TokenLocation(start=0, end=15),
            key=None,
            operator=None,
            value=search_query.StringExpression(
                location=search_query_parser.TokenLocation(start=5, end=15), quotes=None, value="TeSt"
            ),
        )

        query, _extra_info = search_query.az_query_to_opensearch(None, test_tag)
        self.assertEqual(
            query,
            {
                "bool": {
                    "should": [
                        {"term": {"sha256": {"value": "test", "boost": 20}}},
                        {"prefix": {"sha256": "test"}},
                        {"prefix": {"md5": "test"}},
                        {"prefix": {"sha1": "test"}},
                        {"prefix": {"sha512": "test"}},
                        {"prefix": {"ssdeep.hash": "TeSt"}},
                        {"prefix": {"file_format": "TeSt"}},
                        {"prefix": {"file_format_legacy": "TeSt"}},
                        {"prefix": {"magic": "TeSt"}},
                        {"prefix": {"mime": "TeSt"}},
                    ],
                    "minimum_should_match": 1,
                }
            },
        )

        tag2_value = search_query.StringExpression(
            location=search_query_parser.TokenLocation(start=5, end=15), quotes=None, value="testt2"
        )

        test_tag2 = search_query.Tag(
            location=search_query_parser.TokenLocation(start=0, end=15),
            key=search_query.StringExpression(
                location=search_query_parser.TokenLocation(start=0, end=4), quotes=None, value="test2"
            ),
            operator=":",
            value=tag2_value,
        )

        query1, extra_info1 = search_query.az_query_to_opensearch(None, test_tag2)
        extra_info2 = search_query.QueryExtraInfo()
        query2 = search_query._az_field_search_to_opensearch(
            None,
            "test2",
            ":",
            tag2_value,
            extra_info=extra_info2,
        )
        self.assertEqual(
            query1,
            query2,
        )
        self.assertEqual(
            extra_info1,
            extra_info2,
        )
        self.assertFalse(extra_info1.is_binary_tag_search)

        tag3_value = search_query.NumberExpression(
            location=search_query_parser.TokenLocation(start=5, end=15), value=1234
        )

        test_tag3 = search_query.Tag(
            location=search_query_parser.TokenLocation(start=0, end=15),
            key=search_query.StringExpression(
                location=search_query_parser.TokenLocation(start=0, end=4), quotes=None, value="test2"
            ),
            operator=":>",
            value=tag3_value,
        )

        query1, extra_info1 = search_query.az_query_to_opensearch(None, test_tag3)
        extra_info2 = search_query.QueryExtraInfo()
        query2 = search_query._az_field_search_to_opensearch(
            None,
            "test2",
            ":>",
            tag3_value,
            extra_info=extra_info2,
        )

        self.assertEqual(query1, query2)
        self.assertEqual(
            extra_info1,
            extra_info2,
        )
        self.assertFalse(extra_info1.is_binary_tag_search)

    def test_current_node_base_case(self):
        """Tests that the tree walking for Tag elements works correctly."""
        tag_key = search_query.StringExpression(
            location=search_query_parser.TokenLocation(start=0, end=4), quotes=None, value="test2"
        )

        tag_value = search_query.NumberExpression(
            location=search_query_parser.TokenLocation(start=6, end=10), value=1234
        )

        test_tag = search_query.Tag(
            location=search_query_parser.TokenLocation(start=0, end=10),
            key=tag_key,
            operator=":",
            value=tag_value,
        )

        self.assertEqual(
            search_query._current_node(test_tag, 0),
            search_query._TreeWalk(value=tag_key, branch="Key", parents=[test_tag]),
        )

        self.assertEqual(
            search_query._current_node(test_tag, 5),
            search_query._TreeWalk(
                value=test_tag,
                branch=None,
            ),
        )

        self.assertEqual(
            search_query._current_node(test_tag, 9),
            search_query._TreeWalk(value=tag_value, branch="Value", parents=[test_tag]),
        )

        self.assertEqual(search_query._current_node(test_tag, -1), None)

        self.assertEqual(search_query._current_node(test_tag, 11), None)

    def test_current_node_logical_operator(self):
        """Tests that tree walking across logical operators behaves correctly."""
        tag_key = search_query.StringExpression(
            location=search_query_parser.TokenLocation(start=0, end=4), quotes=None, value="test2"
        )

        tag_value = search_query.NumberExpression(
            location=search_query_parser.TokenLocation(start=6, end=10), value=1234
        )

        test_tag = search_query.Tag(
            location=search_query_parser.TokenLocation(start=0, end=10),
            key=tag_key,
            operator=":",
            value=tag_value,
        )

        tag_key2 = search_query.StringExpression(
            location=search_query_parser.TokenLocation(start=15, end=19), quotes=None, value="test2"
        )

        tag_value2 = search_query.NumberExpression(
            location=search_query_parser.TokenLocation(start=21, end=25), value=1234
        )

        test_tag2 = search_query.Tag(
            location=search_query_parser.TokenLocation(start=15, end=25),
            key=tag_key2,
            operator=":",
            value=tag_value2,
        )

        logic_operator = search_query.LogicalOperator(operator="AND", children=[test_tag, test_tag2])

        self.assertEqual(
            search_query._current_node(logic_operator, 0),
            search_query._TreeWalk(value=tag_key, branch="Key", parents=[test_tag, logic_operator]),
        )

        self.assertEqual(
            search_query._current_node(logic_operator, 5),
            search_query._TreeWalk(value=test_tag, branch=None, parents=[logic_operator]),
        )

        self.assertEqual(
            search_query._current_node(logic_operator, 7),
            search_query._TreeWalk(value=tag_value, branch="Value", parents=[test_tag, logic_operator]),
        )

        self.assertEqual(search_query._current_node(logic_operator, 13), None)

        self.assertEqual(
            search_query._current_node(logic_operator, 16),
            search_query._TreeWalk(value=tag_key2, branch="Key", parents=[test_tag2, logic_operator]),
        )

        self.assertEqual(
            search_query._current_node(logic_operator, 20),
            search_query._TreeWalk(value=test_tag2, branch=None, parents=[logic_operator]),
        )

        self.assertEqual(
            search_query._current_node(logic_operator, 22),
            search_query._TreeWalk(value=tag_value2, branch="Value", parents=[test_tag2, logic_operator]),
        )

        self.assertEqual(search_query._current_node(logic_operator, 26), None)

    def test_generate_autocomplete(self):
        """Tests that autocomplete suggestions are produced correctly."""
        self.assertEqual(search_query.generate_autocomplete("", 0), bedr_bauto.AutocompleteInitial())

        # lots of space characters which can cause issues.
        self.assertEqual(search_query.generate_autocomplete("                ", 0), bedr_bauto.AutocompleteInitial())

        self.assertEqual(
            search_query.generate_autocomplete("test:1234", 1),
            bedr_bauto.AutocompleteFieldName(prefix="test", prefix_type="case-insensitive", has_value=True),
        )

        self.assertEqual(
            search_query.generate_autocomplete("test:=1234", 8),
            bedr_bauto.AutocompleteFieldValue(prefix="1234", prefix_type="numeric", key="test"),
        )

        self.assertEqual(
            search_query.generate_autocomplete("test", 2),
            bedr_bauto.AutocompleteFieldValue(prefix="test", prefix_type="case-insensitive", key=None),
        )

        self.assertEqual(
            search_query.generate_autocomplete('test:"Hello"', 9),
            bedr_bauto.AutocompleteFieldValue(prefix="Hello", prefix_type="case-sensitive", key="test"),
        )

        self.assertEqual(
            search_query.generate_autocomplete("test:[5 TO 10]", 9),
            bedr_bauto.AutocompleteFieldValue(
                prefix="5 (inclusive) to 10 (inclusive)", prefix_type="range", key="test"
            ),
        )

        self.assertEqual(
            search_query.generate_autocomplete("test:", 4),
            bedr_bauto.AutocompleteFieldValue(
                prefix="(search for if specified field exists - add a value to search for a value)",
                prefix_type="empty",
                key="test",
            ),
        )

    def test_bad_input_autocomplete(self):
        """Tests that bad inputs are correctly caught and located."""
        self.assertEqual(
            search_query.generate_autocomplete('"Test', 4),
            bedr_bauto.AutocompleteError(
                column=1, message="Unexpected character at col 1 (are quotes/brackets closed?)"
            ),
        )

        self.assertEqual(
            search_query.generate_autocomplete('cat:"Test', 4),
            bedr_bauto.AutocompleteError(
                column=5, message="Unexpected character at col 5 (are quotes/brackets closed?)"
            ),
        )

        self.assertEqual(
            search_query.generate_autocomplete("key:key:key", 4),
            bedr_bauto.AutocompleteError(column=8, message="Unexpected token at col 8"),
        )

    def test_validate_term_query(self):
        dummy_model_valid_keys = ["action", "depth", "file_format", "size"]
        invalid_keys = search_query.validate_term_query('action:"extracted"', dummy_model_valid_keys)
        self.assertEqual(invalid_keys, [])
        invalid_keys = search_query.validate_term_query('action:"extracted" AND size:>10MB', dummy_model_valid_keys)
        self.assertEqual(invalid_keys, [])

        # All Bad values
        invalid_keys = search_query.validate_term_query('action:"extracted" AND badkey:>10MB', dummy_model_valid_keys)
        self.assertCountEqual(invalid_keys, ["badkey"])
        invalid_keys = search_query.validate_term_query('action:"extracted" AND badkey:10MB', dummy_model_valid_keys)
        self.assertCountEqual(invalid_keys, ["badkey"])
        invalid_keys = search_query.validate_term_query(
            'action:"extracted" AND badkey:"value"', dummy_model_valid_keys
        )
        self.assertCountEqual(invalid_keys, ["badkey"])
        invalid_keys = search_query.validate_term_query('action:"extracted" OR badkey:"value"', dummy_model_valid_keys)
        self.assertCountEqual(invalid_keys, ["badkey"])
        invalid_keys = search_query.validate_term_query('badkey:"value" AND badkey2:"value2"', dummy_model_valid_keys)
        self.assertCountEqual(invalid_keys, ["badkey", "badkey2"])
        invalid_keys = search_query.validate_term_query(
            'badkey:"value" AND badkey2:"value2" OR badkey3:value3', dummy_model_valid_keys
        )
        self.assertCountEqual(invalid_keys, ["badkey", "badkey2", "badkey3"])
