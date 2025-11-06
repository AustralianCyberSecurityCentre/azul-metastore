"""Transforms parsed search queries into autocomplete results and OpenSearch queries."""

from typing import Literal, Optional, Tuple

from azul_bedrock.models_restapi import binaries_auto_complete as bedr_bauto
from fastapi import HTTPException
from lark import UnexpectedCharacters, UnexpectedInput, UnexpectedToken
from pydantic import BaseModel

from azul_metastore.common import wrapper
from azul_metastore.common.search_query_parser import (
    Expression,
    FieldComparison,
    LocatedToken,
    LogicalOperator,
    NumberExpression,
    RangeExpression,
    StringExpression,
    Tag,
    parse,
)
from azul_metastore.context import Context

# Magic value used for searching for binary tags
BINARY_TAG_KEY = "binary.tag"
# Magic vaue used for searching for feature tags
FEATURE_TAG_KEY = "feature.tag"


class QueryExtraInfo(BaseModel):
    """Query conversion with additional information added."""

    is_binary_tag_search: bool = False
    is_feature_tag_search: bool = False


def _az_field_search_to_opensearch(
    ctx: Context | None,
    key: str,
    operator: FieldComparison,
    field: StringExpression | NumberExpression | RangeExpression | None,
    extra_info: QueryExtraInfo,
) -> dict:
    """Converts a field search expression to an OpenSearch query."""
    # Handle pre-filter magic expressions
    if key.lower() == BINARY_TAG_KEY or key.lower() == FEATURE_TAG_KEY:
        if ctx is None:
            raise ValueError("Dynamic evaluation needed for tag searches.")

        if not isinstance(field, StringExpression) and not isinstance(field, NumberExpression):
            raise ValueError("Tag search must be made on a string value.")

        if operator != ":" and operator != ":=":
            raise ValueError("Tag search can only be a literal search.")

        tag = str(field.value)

        if key.lower() == BINARY_TAG_KEY:
            extra_info.is_binary_tag_search = True
            # Do a prefilter search for binaries with this tag
            body = {
                "query": {
                    "bool": {
                        "must_not": [{"match": {"state": "disabled"}}],
                        "filter": [{"term": {"type": "entity_tag"}}, {"term": {"tag": tag}}],
                    }
                },
                "sort": [{"timestamp": {"order": "desc"}}],
                "size": 10000,
                "_source": {"includes": ["pivot"]},
            }

            resp = ctx.man.annotation.w.search(ctx.sd, body=body)
            prefilter_binaries = [x["_source"]["pivot"] for x in resp["hits"]["hits"]]

            if not prefilter_binaries:
                raise wrapper.InvalidSearchException("tag not found")

            return {"terms": {"sha256": prefilter_binaries}}
        else:
            extra_info.is_feature_tag_search = True
            # Do a prefilter search for features with this tag
            body = {
                "query": {"bool": {"filter": [{"term": {"type": "fv_tag"}}, {"term": {"tag": tag}}]}},
                "sort": [{"timestamp": {"order": "desc"}}],
                "size": 10000,
                "_source": {"includes": ["feature_name", "feature_value"]},
            }
            resp = ctx.man.annotation.w.search(ctx.sd, body=body)
            prefilter_feature_values = [
                (x["_source"]["feature_name"], x["_source"]["feature_value"]) for x in resp["hits"]["hits"]
            ]

            if not prefilter_feature_values:
                raise wrapper.InvalidSearchException("feature value tag not found")

            query_terms = []
            for name, value in prefilter_feature_values:
                query_terms.append({"term": {f"features_map.{name}": value}})

            return {
                "bool": {
                    "should": query_terms,
                    "minimum_should_match": 1,
                }
            }
    else:
        # Handle searches on generic OpenSearch fields
        if isinstance(field, (StringExpression, NumberExpression)):
            if operator == ":>":
                # Greater than (numeric)
                return {"range": {key: {"gt": field.value}}}
            elif operator == ":<":
                # Less than (numeric)
                return {"range": {key: {"lt": field.value}}}
            elif operator == ":>=":
                # Greater than or equal (numeric)
                return {"range": {key: {"gte": field.value}}}
            elif operator == ":<=":
                # Less than or equal (numeric)
                return {"range": {key: {"lte": field.value}}}
            elif operator == ":=":
                # Exact match
                return {"term": {key: field.value}}
            elif operator == ":":
                # Generic search
                if isinstance(field, StringExpression):
                    # Make this case insensitive if there are not double quotes involved
                    # (single quotes will still be case insensitive)
                    case_insensitive = field.quotes != "double"
                    # See if there is a trailing wildcard
                    if field.value.endswith("*"):
                        # prefix search is somewhat slower than term search
                        return {"prefix": {key: {"value": field.value[0:-1], "case_insensitive": case_insensitive}}}
                    else:
                        return {"term": {key: {"value": field.value, "case_insensitive": case_insensitive}}}
                else:
                    # Case insensitive doesn't make sense for booleans/integers
                    return {"term": {key: field.value}}
        elif isinstance(field, RangeExpression):
            range = {}
            if field.startInclusive:
                range["gte"] = field.start
            else:
                range["gt"] = field.start

            if field.endInclusive:
                range["lte"] = field.end
            else:
                range["lt"] = field.end

            return {"range": {key: range}}
        elif field is None:
            return {"exists": {"field": key}}

    raise ValueError("Hit unreachable during conversion of field")


def _az_query_to_opensearch_with_keys(
    ctx: Context | None, extra_info: QueryExtraInfo, input: Expression
) -> tuple[dict, str | None]:
    """Converts an Azul search query into native OpenSearch syntax."""
    if isinstance(input, LogicalOperator):
        # AND/OR/NOT operator
        keys = []
        transformed_children = []
        for cur_child in input.children:
            transformed_child, cur_key = _az_query_to_opensearch_with_keys(
                ctx=ctx, extra_info=extra_info, input=cur_child
            )
            transformed_children.append(transformed_child)
            if cur_key:
                keys.append(cur_key)

        if input.operator == "AND":
            return {"bool": {"filter": transformed_children}}, None
        elif input.operator == "NOT":
            must_exist_filter_list = []
            for key in keys:
                must_exist_filter_list.append({"exists": {"field": key}})
            return {"bool": {"filter": must_exist_filter_list, "must_not": transformed_children}}, None
        else:
            return {"bool": {"should": transformed_children, "minimum_should_match": 1}}, None
    elif isinstance(input, Tag):
        # Value comparison
        if input.key is None or input.operator is None:
            # This is a global/generic search
            if isinstance(input.value, RangeExpression) or input.value is None:
                # A user shouldn't be able to globally evaluate a range;
                # this doesn't make sense on mainly string fields
                raise ValueError("Search for an implict field should be a literal, not a range")

            term = str(input.value.value)
            return {
                "bool": {
                    "should": [
                        {"term": {"sha256": {"value": term.lower(), "boost": 20}}},
                        {"prefix": {"sha256": term.lower()}},
                        {"prefix": {"md5": term.lower()}},
                        {"prefix": {"sha1": term.lower()}},
                        {"prefix": {"sha512": term.lower()}},
                        {"prefix": {"ssdeep.hash": term}},
                        {"prefix": {"file_format": term}},
                        {"prefix": {"file_format_legacy": term}},
                        {"prefix": {"magic": term}},
                        {"prefix": {"mime": term}},
                    ],
                    # require at least one of the should queries to match
                    # or else will get unrelated binaries, especially if not sorting by score
                    "minimum_should_match": 1,
                }
            }, None
        else:
            # This is a search on a specific field
            return (
                _az_field_search_to_opensearch(
                    ctx=ctx, key=input.key.value, operator=input.operator, field=input.value, extra_info=extra_info
                ),
                input.key.value,
            )


def az_query_to_opensearch(ctx: Context | None, input: Expression) -> tuple[dict, QueryExtraInfo]:
    """Converts an Azul search query into native OpenSearch syntax."""
    extra_info = QueryExtraInfo()
    return _az_query_to_opensearch_with_keys(ctx=ctx, extra_info=extra_info, input=input)[0], extra_info


class _TreeWalk(BaseModel):
    """Results of walking a tree to locate a node while identifying its parents."""

    value: LocatedToken
    # branch is used to identify if the identified token is a key or value
    branch: Optional[Literal["Key"] | Literal["Value"]]
    parents: list[Expression] = []

    def with_parent(self, parent: Expression) -> "_TreeWalk":
        """Returns a copy of this TreeWalk instance with an additional parent."""
        return _TreeWalk(value=self.value, branch=self.branch, parents=[*self.parents, parent])


def _current_node(input: Expression, offset: int) -> Optional[_TreeWalk]:
    """Finds the node in the given expression where the current offset sits, if any."""
    # Walk the tree to determine where the user's caret offset is
    if isinstance(input, LogicalOperator):
        # Logical operator - see if a child element has a hit
        for child in input.children:
            child_result = _current_node(child, offset)
            if child_result is not None:
                return child_result.with_parent(input)
    # Tag - this could contain our character that we are looking for
    elif isinstance(input, Tag) and input.location.contains(offset):
        # This tag contains our location - check to see which branch it might be on
        if input.key is not None and input.key.location.contains(offset):
            return _TreeWalk(value=input.key, branch="Key", parents=[input])

        if input.value is not None and input.value.location.contains(offset):
            return _TreeWalk(value=input.value, branch="Value", parents=[input])

        # If we are here, neither the key nor value contains this tag's value
        # Assume that the user has either selected the operator or whitespace
        return _TreeWalk(value=input, branch=None)

    return None


def _format_expression(
    expr: StringExpression | NumberExpression | RangeExpression,
) -> Tuple[str, bedr_bauto.PrefixType]:
    """Formats an expression in a different manner to how the user inputted it to help spot errors."""
    if isinstance(expr, StringExpression):
        return expr.value, "case-sensitive" if expr.quotes == "double" else "case-insensitive"
    elif isinstance(expr, NumberExpression):
        return str(expr.value), "numeric"
    else:
        return (
            "{} ({}) to {} ({})".format(
                expr.start,
                "inclusive" if expr.startInclusive else "exclusive",
                expr.end,
                "inclusive" if expr.endInclusive else "exclusive",
            ),
            "range",
        )


def generate_autocomplete(input: str, offset: int) -> bedr_bauto.AutocompleteContext:
    """Determines what should be autocompleted based on the current user input state."""
    try:
        parsed_doc = parse(input)
    except UnexpectedToken as e:
        return bedr_bauto.AutocompleteError(column=e.column, message="Unexpected token at col %s" % str(e.column))
    except UnexpectedCharacters as e:
        return bedr_bauto.AutocompleteError(
            column=e.column,
            message="Unexpected character at col %s (are quotes/brackets closed?)" % str(e.column),
        )
    except UnexpectedInput as e:
        return bedr_bauto.AutocompleteError(column=e.column, message=str(e))

    if parsed_doc is None:
        return bedr_bauto.AutocompleteInitial()
    node = _current_node(parsed_doc, offset)

    if node is not None:
        # Determine if this is something we can autocomplete
        if isinstance(node.value, (StringExpression, NumberExpression, RangeExpression)):
            # This is a string we may be able to autocomplete - determine if this is a key
            # or a value
            parent_node = node.parents[0]
            if node.branch == "Value":
                # Autocomplete a value
                if isinstance(parent_node, Tag):
                    # Calculate the fields to use for this autocomplete instance
                    if parent_node.key is not None:
                        key = parent_node.key.value
                    else:
                        key = None

                    prefix, prefix_type = _format_expression(node.value)
                    return bedr_bauto.AutocompleteFieldValue(key=key, prefix=prefix, prefix_type=prefix_type)
                else:
                    raise ValueError(
                        "Parent of a node is not a Tag?", [type(parent_node) for parent_node in node.parents]
                    )
            # Autocomplete a field's key
            elif node.branch == "Key" and isinstance(parent_node, Tag) and isinstance(node.value, StringExpression):
                prefix = node.value.value
                return bedr_bauto.AutocompleteFieldName(
                    has_value=parent_node.value is not None, prefix=prefix, prefix_type="case-insensitive"
                )

        elif isinstance(node.value, Tag):
            # Autocomplete a fields value, where the operator is currently selected
            if node.value.value is not None:
                prefix, prefix_type = _format_expression(node.value.value)
            else:
                prefix = "(search for if specified field exists - add a value to search for a value)"
                prefix_type = "empty"

            if node.value.key is not None:
                key = node.value.key.value
            else:
                key = None
            return bedr_bauto.AutocompleteFieldValue(key=key, prefix=prefix, prefix_type=prefix_type)

    return bedr_bauto.AutocompleteNone()


def _validate_term_query(parse_ast: Expression, model_valid_keys: list[str]) -> list[str]:
    """Check if a term queries keys are found in the model and return any invalid keys."""
    if isinstance(parse_ast, LogicalOperator):
        found_invalid_keys = []
        for cur_child in parse_ast.children:
            found_invalid_keys += _validate_term_query(cur_child, model_valid_keys)
        return found_invalid_keys
    elif isinstance(parse_ast, Tag):
        if parse_ast.key is None or parse_ast.operator is None:
            # This is a global query and we have no way of validating it.
            return []
        else:
            if parse_ast.key.value not in model_valid_keys:
                return [parse_ast.key.value]
    return []


def validate_term_query(term: str, model_valid_keys: list[str]) -> list[str]:
    """Check if a term queries keys are found in the model and return any invalid keys."""
    try:
        parse_ast = parse(term)
    except UnexpectedInput as e:
        raise HTTPException(status_code=400, detail="Failed to parse term: " + str(e))
    return _validate_term_query(parse_ast, model_valid_keys)
