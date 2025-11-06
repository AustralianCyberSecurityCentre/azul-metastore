"""Lucene/LIQE-like binary search parameterisation."""

import contextlib
import functools
from pathlib import Path
from typing import Any, Literal, Optional

from lark import Lark, Token, Transformer
from pydantic import BaseModel

# Table to translate a file size to an integer multiplier
_TRANSLATION_TABLE = {
    "b": 1,
    "kb": 1000,
    "kib": 1024,
    "mb": 1000 * 1000,
    "mib": 1024 * 1024,
    "gb": 1000 * 1000 * 1000,
    "gib": 1024 * 1024 * 1024,
    "tb": 1000 * 1000 * 1000 * 1000,
    "tib": 1024 * 1024 * 1024 * 1024,
}


def translate_size_unit(value: int | float, unit: str) -> int | float:
    """Allow the user to specify KB/MB/etc, which gets converted into an integer unit.

    :param value: The user string to convert.
    :param unit: The file size multipler to use.
    :return: An integer.
    """
    return value * _TRANSLATION_TABLE[unit.lower()]


_GRAMMAR_PATH = Path(__file__).with_name("grammar.lark")

with _GRAMMAR_PATH.open("r") as file:
    _GRAMMAR = file.read()


class TokenLocation(BaseModel):
    """Start/end of a token."""

    start: int
    end: int

    def contains(self, caret: int):
        """Returns if the given caret falls within this token's location."""
        return self.start <= caret and caret < self.end


def _token_to_location(token: Token | TokenLocation) -> TokenLocation:
    """Converts a token to a location."""
    if isinstance(token, TokenLocation):
        return token

    start = token.start_pos
    end = token.end_pos
    if not isinstance(start, int) or not isinstance(end, int):
        raise ValueError("Token missing start/end")

    return TokenLocation(start=start, end=end)


def _combine_locations(*args: TokenLocation) -> TokenLocation:
    """Combines a series of locations."""
    return functools.reduce(
        lambda a, b: TokenLocation(
            start=min(a.start, b.start),
            end=max(a.end, b.end),
        ),
        args,
    )


class LocatedToken(BaseModel):
    """A token with a location."""

    location: TokenLocation


class _RawStringToken(LocatedToken):
    """A floating token that is yet to be attached to somewhere in the AST."""

    value: str


class _RawIntToken(LocatedToken):
    """A floating token that is yet to be attached to somewhere in the AST."""

    value: int | float


class _RangeInclusiveMarker(LocatedToken):
    """A marker for whether either side of a range is inclusive or exclusive."""

    value: bool


class StringExpression(LocatedToken):
    """A string expression contains a string and source metadata."""

    quotes: Optional[Literal["double"] | Literal["single"]]
    value: str


class NumberExpression(LocatedToken):
    """A comparison between a number and a field."""

    value: int | float


class RangeExpression(LocatedToken):
    """A comparison between a field's value and a range."""

    start: int
    startInclusive: bool
    end: int
    endInclusive: bool


# Range of tokens available for field comparisons
FieldComparison = Literal[":"] | Literal[":="] | Literal[":>"] | Literal[":<"] | Literal[":>="] | Literal[":<="]


class Tag(LocatedToken):
    """A tag is an individual search term for a field."""

    key: Optional[StringExpression] = None
    operator: Optional[FieldComparison] = None
    value: Optional[StringExpression | NumberExpression | RangeExpression]


class LogicalOperator(BaseModel):
    """A logical operator is a boolean operation between two fields."""

    operator: Literal["OR"] | Literal["AND"] | Literal["NOT"]
    children: list["Expression"]


Expression = LogicalOperator | Tag

# Rebuild recursive structure
LogicalOperator.model_rebuild()

# Translations for various escape sequences
_ESCAPED_CHARACTERS = {"n": "\n", "r": "\r", "t": "\t", "\\": "\\", '"': '"', "'": "'"}


def _unescape_string(input: str, quote_character: Optional[str]) -> str:
    """Unescapes a regex captured string."""
    if quote_character is not None:
        # Remove starting quotes
        if input.startswith(quote_character):
            input = input[1:]

        if input.endswith(quote_character):
            input = input[0:-1]

    # Unescape any escape sequences
    output = ""
    capturing_escape = False

    for character in input:
        if capturing_escape:
            # Look up the character in the lookup table
            if character not in _ESCAPED_CHARACTERS:
                raise ValueError("Invalid escape character: " + character)

            output += _ESCAPED_CHARACTERS[character]
            capturing_escape = False
        elif character == "\\":
            capturing_escape = True
        else:
            output += character

    if capturing_escape:
        raise ValueError("Unterminated escape at end of string")

    return output


class AzTransformer(Transformer):
    """A transformer for Azul's search language."""

    def _assert_input_tokens(self, tokens: list, length: int):
        """Assert that the number of input tokens matches expectations."""
        if len(tokens) != length:
            raise ValueError("Input tokens don't match expected length")

    def number_token(self, tokens: list[Token]):
        """Handles values for tags which are numbers."""
        # Return an int value
        self._assert_input_tokens(tokens, 1)
        if "." in tokens[0].value:
            parsed = float(tokens[0].value)
        else:
            parsed = int(tokens[0].value)
        return _RawIntToken(value=parsed, location=_token_to_location(tokens[0]))

    def _parse_string(self, tokens: list[Token], quotes: Optional[Literal["double"] | Literal["single"]]):
        """Handles a string for a key or value that may have quotes."""
        # If there are no quotes, and this string is just numbers, try to parse as such
        if quotes is None:
            with contextlib.suppress(ValueError):
                return self.number_expression([self.number_token(tokens)])

        # There is only one token in a string
        self._assert_input_tokens(tokens, 1)
        inner_token: str = tokens[0].value

        quote_character = None
        if quotes == "double":
            quote_character = '"'
        elif quotes == "single":
            quote_character = "'"

        return StringExpression(
            value=_unescape_string(inner_token, quote_character), quotes=quotes, location=_token_to_location(tokens[0])
        )

    def tag_string_single_quoted(self, tokens: list[Token]):
        """Handles a string for a key or value that has single quotes."""
        return self._parse_string(tokens, "single")

    def tag_string_double_quoted(self, tokens: list[Token]):
        """Handles a string for a key or value that has double quotes."""
        # There is only one token in a double quoted string
        return self._parse_string(tokens, "double")

    def tag_string_unquoted(self, tokens: list[Token]):
        """Handles a string for a key or value that doesn't have quotes."""
        return self._parse_string(tokens, None)

    def filesize_suffix(self, tokens: list[Token]):
        """Handles a filesize suffix token."""
        # Pass through the expression as is; it will be parsed by number_expression
        return _RawStringToken(value=tokens[0].value, location=_token_to_location(tokens[0]))

    def inclusive_start_operator(self, tokens) -> _RangeInclusiveMarker:
        """Handles the start of a range that is inclusive."""
        # Inclusive = true
        return _RangeInclusiveMarker(value=True, location=_token_to_location(tokens[0]))

    def inclusive_end_operator(self, tokens) -> _RangeInclusiveMarker:
        """Handles the end of a range that is inclusive."""
        # Inclusive = true
        return _RangeInclusiveMarker(value=True, location=_token_to_location(tokens[0]))

    def exclusive_start_operator(self, tokens) -> _RangeInclusiveMarker:
        """Handles the start of a range that is exclusive."""
        # Exclusive = false
        return _RangeInclusiveMarker(value=False, location=_token_to_location(tokens[0]))

    def exclusive_end_operator(self, tokens) -> _RangeInclusiveMarker:
        """Handles the end of a range that is exclusive."""
        # Exclusive = false
        return _RangeInclusiveMarker(value=False, location=_token_to_location(tokens[0]))

    def generic_comparison(self, _tokens) -> FieldComparison:
        """Handles : tokens."""
        return ":"

    def exact_comparison(self, _tokens) -> FieldComparison:
        """Handles := tokens."""
        return ":="

    def gt_comparison(self, _tokens) -> FieldComparison:
        """Handles > tokens."""
        return ":>"

    def le_comparison(self, _tokens) -> FieldComparison:
        """Handles < tokens."""
        return ":<"

    def gte_comparison(self, _tokens) -> FieldComparison:
        """Handles >= tokens."""
        return ":>="

    def lte_comparison(self, _tokens) -> FieldComparison:
        """Handles <= tokens."""
        return ":<="

    def number_expression(self, tokens: list[_RawIntToken | _RawStringToken]) -> NumberExpression:
        """Converts number expressions into a native type."""
        value_token = tokens[0]
        if not isinstance(value_token.value, int) and not isinstance(value_token.value, float):
            raise ValueError("Internal error: RawToken of numeric field is not an integer")

        # This can contain 1 OR 2 tokens
        if len(tokens) == 1:
            # Plain number
            return NumberExpression(value=value_token.value, location=value_token.location)
        elif len(tokens) == 2:
            # Filesize
            size_token = tokens[1]
            if not isinstance(size_token.value, str):
                raise ValueError("Internal error: RawToken of string field is not an string")

            return NumberExpression(
                value=translate_size_unit(value_token.value, size_token.value),
                location=_combine_locations(value_token.location, size_token.location),
            )
        else:
            raise ValueError("Unexpected count of tokens when parsing number expression")

    number_or_filesize_expression = number_expression

    def range_expression(self, tokens: list[_RangeInclusiveMarker | NumberExpression]) -> RangeExpression:
        """Converts range expressions into a native type."""
        # This will contain an inclusive/exclusive start marker, start value, end value and another end marker
        self._assert_input_tokens(tokens, 4)

        if (
            not isinstance(tokens[0], _RangeInclusiveMarker)
            or not isinstance(tokens[1], NumberExpression)
            or not isinstance(tokens[2], NumberExpression)
            or not isinstance(tokens[3], _RangeInclusiveMarker)
        ):
            raise ValueError("Invalid types passed to range expression")

        return RangeExpression(
            startInclusive=tokens[0].value,
            start=int(tokens[1].value),
            end=int(tokens[2].value),
            endInclusive=tokens[3].value,
            location=_combine_locations(*[_token_to_location(token.location) for token in tokens]),
        )

    def raw_tag(self, keys: list[StringExpression | NumberExpression | RangeExpression]):
        """Handles tags without any keys."""
        # This token will have a single value and no key
        self._assert_input_tokens(keys, 1)
        return Tag(location=keys[0].location, value=keys[0])

    def key_value_tag(self, children: list[Any]):
        """Handles a tag token with both a key and value."""
        # This token will take a key, an operator and an (optional) value
        if len(children) != 2 and len(children) != 3:
            raise ValueError("Input tokens don't match expected length")

        key: StringExpression = children[0]
        operator: FieldComparison = children[1]
        if len(children) == 3:
            value: Optional[StringExpression | NumberExpression | RangeExpression] = children[2]
        else:
            value = None

        if value is not None:
            location = _combine_locations(key.location, value.location)
        else:
            location = key.location.model_copy()
            location.end += len(operator)

        return Tag(key=key, operator=operator, value=value, location=location)

    def tag_value(self, keys: list[Any]):
        """Handles a tag's value token."""
        # Wrapper for a range of different types, pass through as-is
        self._assert_input_tokens(keys, 1)
        return keys[0]

    def expression_excl_logic(self, expr_operands: list[Any]):
        """Handles the expression subset token expression_excl_logic."""
        # This is just a wrapper
        self._assert_input_tokens(expr_operands, 1)
        return expr_operands[0]

    def _handle_logical_expr(self, children: list[Expression], operator: Literal["OR"] | Literal["AND"]) -> Expression:
        """Helper for logical expressions to flatten operators where possible."""
        # This can have any number of elements
        if len(children) == 1:
            return children[0]

        # Flatten this expression if possible - this makes generated OpenSearch code easier to understand
        cleaned_children = []
        for child in children:
            if isinstance(child, LogicalOperator) and child.operator == operator:
                cleaned_children += child.children
            else:
                cleaned_children.append(child)

        return LogicalOperator(children=cleaned_children, operator=operator)

    def or_logical_expr(self, children: list[Expression]):
        """Handles OR logical expression tokens."""
        return self._handle_logical_expr(children, "OR")

    def and_logical_expr(self, children: list[Expression]):
        """Handles AND logical expression tokens."""
        return self._handle_logical_expr(children, "AND")

    def not_logical_expr(self, children: list[Expression]):
        """Handles NOT logical expressions tokens."""
        # Not should only have a single child
        self._assert_input_tokens(children, 1)

        return LogicalOperator(children=children, operator="NOT")

    def parenthesised_expr(self, child: list[Expression]):
        """Handles parenthesised expressions tokens."""
        # This is only useful when determining order of operations when parsing
        # Pass through the child as-is
        self._assert_input_tokens(child, 1)
        return child[0]

    def main(self, children: list[Any]):
        """Handles the main token."""
        # 'main' may contain multiple children - treat as an implicit AND
        return self._handle_logical_expr(children, "AND")


# LALR used as the parser as the grammar has been designed to not be ambigious
# (which makes it portable to runtimes that don't have Earley) + is a little faster
_LARK_PARSER = Lark(_GRAMMAR, parser="lalr", transformer=AzTransformer())


def parse(input: str) -> Optional[Expression]:
    """Parses an Azul search expression."""
    stripped_input = input.rstrip()
    if len(stripped_input) == 0:
        return None
    # We know that the output of this will be an Expression (or an Exception)
    return _LARK_PARSER.parse(stripped_input)  # type: ignore
