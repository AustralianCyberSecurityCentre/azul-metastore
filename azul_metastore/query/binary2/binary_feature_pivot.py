"""Feature pivoting queries."""

from azul_bedrock.models_restapi import features as bedr_features

from azul_metastore.context import Context
from azul_metastore.query.plugin import find_features

MAX_SHA256_MATCHES = 10000


def find_common_features_from_features(
    ctx: Context,
    feature_values: list[bedr_features.FeaturePivotRequest],
) -> bedr_features.FeaturePivotResponse:
    """Find common features based on provided features."""
    # Create a filter per feature value, to behave as an AND filtering.
    feature_value_filtering = []
    for feat_val in feature_values:
        feature_value_filtering.append(
            {
                "has_child": {
                    "type": "metadata",
                    "query": {"term": {f"features_map.{feat_val.feature_name}": feat_val.feature_value}},
                }
            }
        )

    body = {
        "query": {"bool": {"filter": feature_value_filtering}},
        "size": MAX_SHA256_MATCHES,
        "_source": False,
    }
    binaries_with_features = ctx.man.binary2.w.search(ctx.sd, body)
    found_hits = binaries_with_features.get("hits", {}).get("hits")

    # If there are no matches nothing is found.
    if not found_hits:
        return bedr_features.FeaturePivotResponse(
            feature_value_counts=[], incomplete_query=False, reason="No matches found"
        )

    matching_sha256s = []
    for h in found_hits:
        hit_sha256 = h["_id"]
        matching_sha256s.append(hit_sha256)

    if len(matching_sha256s) >= MAX_SHA256_MATCHES:
        incomplete_query = True
        reason_incomplete = (
            "The initial search for binaries matching the selected features was greater than 10,000."
            + " Refine your feature selection to reduce count."
        )
    else:
        incomplete_query = False
        reason_incomplete = ""

    # Get all the features that exist in Azul
    azul_known_features = find_features(ctx)

    # Aggregations for all the features that exist in Azul with the provided binaries.
    all_aggregations = {}
    feature_descriptions = dict()
    for feat in azul_known_features:
        all_aggregations[feat.name] = {"terms": {"field": f"features_map.{feat.name}", "min_doc_count": 2}}
        if feat.descriptions:
            feature_descriptions[feat.name] = feat.descriptions[0].desc

    agg_body = {
        "query": {"bool": {"filter": [{"terms": {"sha256": matching_sha256s}}]}},
        "aggs": all_aggregations,
        "size": 0,
    }
    aggregration_result = ctx.man.binary2.w.search(ctx.sd, agg_body)

    fpnwvc: list[bedr_features.FeaturePivotNameWithValueCount] = []
    for feature_name, model in aggregration_result.get("aggregations", {}).items():
        current_feat_value_count: list[bedr_features.FeaturePivotValueCount] = list()
        for bucket_values in model.get("buckets"):
            found_value = bucket_values.get("key")
            number_of_matching_binaries = bucket_values.get("doc_count")
            # Omit value if the value is empty or the number of matching values isn't known.
            if not found_value or not number_of_matching_binaries:
                continue

            current_feat_value_count.append(
                bedr_features.FeaturePivotValueCount(
                    feature_value=found_value, entity_count=number_of_matching_binaries
                )
            )
        # Only include in result if it had values.
        if current_feat_value_count:
            fpnwvc.append(
                bedr_features.FeaturePivotNameWithValueCount(
                    feature_name=feature_name,
                    values_and_counts=current_feat_value_count,
                    feature_description=feature_descriptions.get(feature_name, ""),
                )
            )

    return bedr_features.FeaturePivotResponse(
        feature_value_counts=fpnwvc, incomplete_query=incomplete_query, reason=reason_incomplete
    )
