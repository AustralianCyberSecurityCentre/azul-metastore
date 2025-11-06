"""Tests the Azul query language parser."""

from lark import Token

from azul_metastore.common import search_query_parser
from tests.support import unit_test


class TestSearchQueryParser(unit_test.BaseUnitTestCase):
    def test_translate_size_unit(self):
        """Tests translating size units in a strict context."""
        self.assertEqual(search_query_parser.translate_size_unit(1, "b"), 1)
        self.assertEqual(search_query_parser.translate_size_unit(1, "kb"), 1000)
        self.assertEqual(search_query_parser.translate_size_unit(1, "kb"), 1000)
        self.assertEqual(search_query_parser.translate_size_unit(10, "kb"), 10000)
        self.assertEqual(search_query_parser.translate_size_unit(1000, "kb"), 1000000)
        self.assertEqual(search_query_parser.translate_size_unit(1, "MB"), 1000000)
        self.assertEqual(search_query_parser.translate_size_unit(1, "MiB"), 1048576)
        self.assertEqual(search_query_parser.translate_size_unit(1, "gb"), 1000000000)
        self.assertEqual(search_query_parser.translate_size_unit(1, "gIb"), 1073741824)

    def test_token_to_location(self):
        """Tests that Lark Tokens can be converted to native TokenLocations."""
        # Test that native tokens work
        self.assertEqual(
            search_query_parser._token_to_location(Token("Test", "value", start_pos=5, end_pos=10)),
            search_query_parser.TokenLocation(start=5, end=10),
        )

        # Test that TokenLocations are passed through as-is
        self.assertEqual(
            search_query_parser._token_to_location(search_query_parser.TokenLocation(start=5, end=10)),
            search_query_parser.TokenLocation(start=5, end=10),
        )

    def test_combine_locations(self):
        """Tests that locations can be combined to be the composite of each."""
        # Test that adjacent locations are merged correctly
        self.assertEqual(
            search_query_parser._combine_locations(
                search_query_parser.TokenLocation(start=10, end=15), search_query_parser.TokenLocation(start=5, end=10)
            ),
            search_query_parser.TokenLocation(start=5, end=15),
        )

        self.assertEqual(
            search_query_parser._combine_locations(
                search_query_parser.TokenLocation(start=1, end=1), search_query_parser.TokenLocation(start=2, end=2)
            ),
            search_query_parser.TokenLocation(start=1, end=2),
        )

        # Test that non-adjacent locations are also merged correctly (with the maximum extent for the locations selected)
        self.assertEqual(
            search_query_parser._combine_locations(
                search_query_parser.TokenLocation(start=1, end=1), search_query_parser.TokenLocation(start=3, end=3)
            ),
            search_query_parser.TokenLocation(start=1, end=3),
        )

    def test_unescape_string(self):
        """Test that strings are correctly unescaped."""
        self.assertEqual(search_query_parser._unescape_string('"Hello, World!"', '"'), "Hello, World!")

        self.assertEqual(search_query_parser._unescape_string('"Hello, \\\\World!"', '"'), "Hello, \\World!")
        self.assertEqual(search_query_parser._unescape_string('"Hello, \\nWorld!"', '"'), "Hello, \nWorld!")

        self.assertEqual(search_query_parser._unescape_string("Hello, World!", '"'), "Hello, World!")
        self.assertEqual(search_query_parser._unescape_string("'Hello, World!'", "'"), "Hello, World!")
        self.assertEqual(search_query_parser._unescape_string('"Hello, World!"', "'"), '"Hello, World!"')

    def test_tag(self):
        """Tests that tag queries get transformed correctly."""
        self.assertEqual(
            search_query_parser.parse("cats"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=4),
                value=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=4), quotes=None, value="cats"
                ),
            ),
        )

    def test_tag_numeric_hash(self):
        """Tests that hashes on their own are handled correctly."""
        # Base case
        self.assertEqual(
            search_query_parser.parse("abcd1234"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=8),
                value=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=8), quotes=None, value="abcd1234"
                ),
            ),
        )

        # Hash that starts with numbers
        self.assertEqual(
            search_query_parser.parse("1234abcd"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=8),
                value=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=8), quotes=None, value="1234abcd"
                ),
            ),
        )

    def test_tag_key_value(self):
        """Tests that tags with a specified key/value pair behave correctly."""
        self.assertEqual(
            search_query_parser.parse("entity.name:animals"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=19),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=11), quotes=None, value="entity.name"
                ),
                operator=":",
                value=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=12, end=19), quotes=None, value="animals"
                ),
            ),
        )

    def test_tag_key_value_numeric(self):
        """Tests that tags with a numeric key/value pair behave correctly."""
        self.assertEqual(
            search_query_parser.parse("entity.name:1234"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=16),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=11), quotes=None, value="entity.name"
                ),
                operator=":",
                value=search_query_parser.NumberExpression(
                    location=search_query_parser.TokenLocation(start=12, end=16), value=1234
                ),
            ),
        )

        self.assertEqual(
            search_query_parser.parse('entity.name:"1234"'),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=18),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=11), quotes=None, value="entity.name"
                ),
                operator=":",
                value=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=12, end=18), quotes="double", value="1234"
                ),
            ),
        )

        self.assertEqual(
            search_query_parser.parse("entity.name:1234.5"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=18),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=11), quotes=None, value="entity.name"
                ),
                operator=":",
                value=search_query_parser.NumberExpression(
                    location=search_query_parser.TokenLocation(start=12, end=18), value=1234.5
                ),
            ),
        )

    def test_tag_key_value_operator(self):
        """Tests tags with a numeric key/value pair when in a greater than or less than config."""
        self.assertEqual(
            search_query_parser.parse("entity.name:>1234"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=17),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=11), quotes=None, value="entity.name"
                ),
                operator=":>",
                value=search_query_parser.NumberExpression(
                    location=search_query_parser.TokenLocation(start=13, end=17), value=1234
                ),
            ),
        )

        self.assertEqual(
            search_query_parser.parse("entity.name:<1234.5"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=19),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=11), quotes=None, value="entity.name"
                ),
                operator=":<",
                value=search_query_parser.NumberExpression(
                    location=search_query_parser.TokenLocation(start=13, end=19), value=1234.5
                ),
            ),
        )

        self.assertEqual(
            search_query_parser.parse("entity.name:=1234"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=17),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=11), quotes=None, value="entity.name"
                ),
                operator=":=",
                value=search_query_parser.NumberExpression(
                    location=search_query_parser.TokenLocation(start=13, end=17), value=1234
                ),
            ),
        )

        self.assertEqual(
            search_query_parser.parse("entity.name:>=1234"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=18),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=11), quotes=None, value="entity.name"
                ),
                operator=":>=",
                value=search_query_parser.NumberExpression(
                    location=search_query_parser.TokenLocation(start=14, end=18), value=1234
                ),
            ),
        )

        self.assertEqual(
            search_query_parser.parse("entity.name:<=1234"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=18),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=11), quotes=None, value="entity.name"
                ),
                operator=":<=",
                value=search_query_parser.NumberExpression(
                    location=search_query_parser.TokenLocation(start=14, end=18), value=1234
                ),
            ),
        )

    def test_tag_key_value_filesize(self):
        """Tests that tags with a filesize key/value pair behave correctly."""
        self.assertEqual(
            search_query_parser.parse("entity.name:=50MB"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=17),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=11), quotes=None, value="entity.name"
                ),
                operator=":=",
                value=search_query_parser.NumberExpression(
                    location=search_query_parser.TokenLocation(start=13, end=17), value=50 * 1000 * 1000
                ),
            ),
        )

    def test_tag_quoted(self):
        """Tests that tags with quotes are parsed correctly."""
        self.assertEqual(
            search_query_parser.parse('"cats"'),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=6),
                value=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=6), quotes="double", value="cats"
                ),
            ),
        )

        self.assertEqual(
            search_query_parser.parse("'cats'"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=6),
                value=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=6), quotes="single", value="cats"
                ),
            ),
        )

    def test_whitespace_handling(self):
        """Tests that whitespace handling behaves correctly."""
        # This is one of the most common failure points
        self.assertEqual(
            search_query_parser.parse(" entity.name:animals"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=1, end=20),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=1, end=12), quotes=None, value="entity.name"
                ),
                operator=":",
                value=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=13, end=20), quotes=None, value="animals"
                ),
            ),
        )

        self.assertEqual(
            search_query_parser.parse(" entity.name:animals "),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=1, end=20),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=1, end=12), quotes=None, value="entity.name"
                ),
                operator=":",
                value=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=13, end=20), quotes=None, value="animals"
                ),
            ),
        )

        self.assertEqual(
            search_query_parser.parse("entity.name:animals "),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=19),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=11), quotes=None, value="entity.name"
                ),
                operator=":",
                value=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=12, end=19), quotes=None, value="animals"
                ),
            ),
        )

    def test_logical_or_operator(self):
        """Tests that OR logical operators on multiple tags behave correctly."""
        self.assertEqual(
            search_query_parser.parse("cats OR 'dogs'"),
            search_query_parser.LogicalOperator(
                operator="OR",
                children=[
                    search_query_parser.Tag(
                        location=search_query_parser.TokenLocation(start=0, end=4),
                        value=search_query_parser.StringExpression(
                            location=search_query_parser.TokenLocation(start=0, end=4), quotes=None, value="cats"
                        ),
                    ),
                    search_query_parser.Tag(
                        location=search_query_parser.TokenLocation(start=8, end=14),
                        value=search_query_parser.StringExpression(
                            location=search_query_parser.TokenLocation(start=8, end=14), quotes="single", value="dogs"
                        ),
                    ),
                ],
            ),
        )

        self.assertEqual(
            search_query_parser.parse("cats OR dogs OR mice"),
            search_query_parser.LogicalOperator(
                operator="OR",
                children=[
                    search_query_parser.Tag(
                        location=search_query_parser.TokenLocation(start=0, end=4),
                        value=search_query_parser.StringExpression(
                            location=search_query_parser.TokenLocation(start=0, end=4), quotes=None, value="cats"
                        ),
                    ),
                    search_query_parser.Tag(
                        location=search_query_parser.TokenLocation(start=8, end=12),
                        value=search_query_parser.StringExpression(
                            location=search_query_parser.TokenLocation(start=8, end=12), quotes=None, value="dogs"
                        ),
                    ),
                    search_query_parser.Tag(
                        location=search_query_parser.TokenLocation(start=16, end=20),
                        value=search_query_parser.StringExpression(
                            location=search_query_parser.TokenLocation(start=16, end=20), quotes=None, value="mice"
                        ),
                    ),
                ],
            ),
        )

    def test_logical_and_operator(self):
        """Tests that AND logical operators on multiple tags behave correctly."""
        self.assertEqual(
            search_query_parser.parse("cats AND 'dogs'"),
            search_query_parser.LogicalOperator(
                operator="AND",
                children=[
                    search_query_parser.Tag(
                        location=search_query_parser.TokenLocation(start=0, end=4),
                        value=search_query_parser.StringExpression(
                            location=search_query_parser.TokenLocation(start=0, end=4), quotes=None, value="cats"
                        ),
                    ),
                    search_query_parser.Tag(
                        location=search_query_parser.TokenLocation(start=9, end=15),
                        value=search_query_parser.StringExpression(
                            location=search_query_parser.TokenLocation(start=9, end=15), quotes="single", value="dogs"
                        ),
                    ),
                ],
            ),
        )

        self.assertEqual(
            search_query_parser.parse("cats AND dogs AND mice"),
            search_query_parser.LogicalOperator(
                operator="AND",
                children=[
                    search_query_parser.Tag(
                        location=search_query_parser.TokenLocation(start=0, end=4),
                        value=search_query_parser.StringExpression(
                            location=search_query_parser.TokenLocation(start=0, end=4), quotes=None, value="cats"
                        ),
                    ),
                    search_query_parser.Tag(
                        location=search_query_parser.TokenLocation(start=9, end=13),
                        value=search_query_parser.StringExpression(
                            location=search_query_parser.TokenLocation(start=9, end=13), quotes=None, value="dogs"
                        ),
                    ),
                    search_query_parser.Tag(
                        location=search_query_parser.TokenLocation(start=18, end=22),
                        value=search_query_parser.StringExpression(
                            location=search_query_parser.TokenLocation(start=18, end=22), quotes=None, value="mice"
                        ),
                    ),
                ],
            ),
        )

    def test_logical_not_operator(self):
        """Tests that the NOT operator is correctly applied to a tag."""
        self.assertEqual(
            search_query_parser.parse("NOT cats"),
            search_query_parser.LogicalOperator(
                operator="NOT",
                children=[
                    search_query_parser.Tag(
                        location=search_query_parser.TokenLocation(start=4, end=8),
                        value=search_query_parser.StringExpression(
                            location=search_query_parser.TokenLocation(start=4, end=8), quotes=None, value="cats"
                        ),
                    )
                ],
            ),
        )

        self.assertEqual(
            search_query_parser.parse("!cats"),
            search_query_parser.LogicalOperator(
                operator="NOT",
                children=[
                    search_query_parser.Tag(
                        location=search_query_parser.TokenLocation(start=1, end=5),
                        value=search_query_parser.StringExpression(
                            location=search_query_parser.TokenLocation(start=1, end=5), quotes=None, value="cats"
                        ),
                    )
                ],
            ),
        )

    def test_not_order_of_operations(self):
        """Tests that NOTs are interpreted first in ambigious expressions."""
        self.assertEqual(
            search_query_parser.parse("NOT cats AND NOT fish"),
            search_query_parser.LogicalOperator(
                operator="AND",
                children=[
                    search_query_parser.LogicalOperator(
                        operator="NOT",
                        children=[
                            search_query_parser.Tag(
                                location=search_query_parser.TokenLocation(start=4, end=8),
                                value=search_query_parser.StringExpression(
                                    location=search_query_parser.TokenLocation(start=4, end=8),
                                    quotes=None,
                                    value="cats",
                                ),
                            )
                        ],
                    ),
                    search_query_parser.LogicalOperator(
                        operator="NOT",
                        children=[
                            search_query_parser.Tag(
                                location=search_query_parser.TokenLocation(start=17, end=21),
                                value=search_query_parser.StringExpression(
                                    location=search_query_parser.TokenLocation(start=17, end=21),
                                    quotes=None,
                                    value="fish",
                                ),
                            )
                        ],
                    ),
                ],
            ),
        )

    def test_range(self):
        """Tests that ranges are correctly handled."""
        self.assertEqual(
            search_query_parser.parse("entity.file_size:[10 TO 20]"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=27),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=16),
                    quotes=None,
                    value="entity.file_size",
                ),
                operator=":",
                value=search_query_parser.RangeExpression(
                    location=search_query_parser.TokenLocation(start=17, end=27),
                    startInclusive=True,
                    start=10,
                    endInclusive=True,
                    end=20,
                ),
            ),
        )

        self.assertEqual(
            search_query_parser.parse("entity.file_size:(10 TO 20]"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=0, end=27),
                key=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=0, end=16),
                    quotes=None,
                    value="entity.file_size",
                ),
                operator=":",
                value=search_query_parser.RangeExpression(
                    location=search_query_parser.TokenLocation(start=17, end=27),
                    startInclusive=False,
                    start=10,
                    endInclusive=True,
                    end=20,
                ),
            ),
        )

    def test_parenthesised_expr(self):
        """Tests an expression that has parentheses."""
        self.assertEqual(
            search_query_parser.parse("(cats)"),
            search_query_parser.Tag(
                location=search_query_parser.TokenLocation(start=1, end=5),
                value=search_query_parser.StringExpression(
                    location=search_query_parser.TokenLocation(start=1, end=5), quotes=None, value="cats"
                ),
            ),
        )

    def test_parenthesised_expr_ordering(self):
        """Tests an expression with parentheses combined with other logic operators."""
        self.assertEqual(
            search_query_parser.parse("(cats) or dogs"),
            search_query_parser.LogicalOperator(
                operator="OR",
                children=[
                    search_query_parser.Tag(
                        location=search_query_parser.TokenLocation(start=1, end=5),
                        value=search_query_parser.StringExpression(
                            location=search_query_parser.TokenLocation(start=1, end=5), quotes=None, value="cats"
                        ),
                    ),
                    search_query_parser.Tag(
                        location=search_query_parser.TokenLocation(start=10, end=14),
                        value=search_query_parser.StringExpression(
                            location=search_query_parser.TokenLocation(start=10, end=14), quotes=None, value="dogs"
                        ),
                    ),
                ],
            ),
        )
