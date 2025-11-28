from azul_bedrock.models_restapi import features as bedr_features

from tests.support import gen, integration_test


class TestFeatures(integration_test.BaseRestapi):
    def test_known_bad_feature_pivots(self):
        # --- Search for features that don't exist.
        response = self.client.post(
            "/v0/features/pivot",
            json={
                "feature_values": [
                    bedr_features.FeaturePivotRequest(feature_name="f99", feature_value="v99").model_dump(),
                ]
            },
        )

        self.assertEqual(200, response.status_code)
        print(f"Expected: {response.json()["data"]}")
        self.assertEqual(
            {
                "feature_value_counts": [],
                "incomplete_query": False,
                "reason": "No matches found",
            },
            response.json()["data"],
        )

        # --- Search with no features
        response = self.client.post(
            "/v0/features/pivot",
            json={"feature_values": []},
        )
        self.assertEqual(422, response.status_code)

    def test_pivot_features(self):
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", authornv=("a1", "1"), fvl=[("f1", "v1"), ("f1", "v2"), ("f1", "v3")]),
                gen.binary_event(eid="e2", authornv=("a1", "1"), fvl=[("f1", "v1"), ("f1", "v2")]),
                gen.binary_event(eid="e3", authornv=("a1", "2"), fvl=[("f1", "v1"), ("f3", "v1")]),
                gen.binary_event(
                    eid="e4", authornv=("a2", "1"), fvtl=[("f2", "http://blah.com", "uri"), ("f4", "v4", "string")]
                ),
                gen.binary_event(
                    eid="e5", authornv=("a2", "1"), fvtl=[("f2", "http://blah.com", "uri"), ("f5", "v5", "string")]
                ),
            ]
        )
        # --- Search for known feature values that match with multiple binaries.
        response = self.client.post(
            "/v0/features/pivot",
            json={
                "feature_values": [
                    bedr_features.FeaturePivotRequest(feature_name="f1", feature_value="v1").model_dump(),
                    bedr_features.FeaturePivotRequest(feature_name="f1", feature_value="v2").model_dump(),
                ]
            },
        )

        self.assertEqual(200, response.status_code)
        print(f"Expected: {response.json()["data"]}")
        self.assertEqual(
            {
                "feature_value_counts": [
                    {
                        "feature_name": "f1",
                        "feature_description": "generic_description",
                        "values_and_counts": [
                            {"feature_value": "v1", "entity_count": "2"},
                            {"feature_value": "v2", "entity_count": "2"},
                        ],
                    }
                ],
                "incomplete_query": False,
                "reason": "",
            },
            response.json()["data"],
        )

        # Test single feature with no friends
        response = self.client.post(
            "/v0/features/pivot",
            json={
                "feature_values": [
                    bedr_features.FeaturePivotRequest(feature_name="f3", feature_value="v1").model_dump(),
                ]
            },
        )

        self.assertEqual(200, response.status_code)
        print(f"Expected: {response.json()["data"]}")
        self.assertEqual(
            {"feature_value_counts": [], "incomplete_query": False, "reason": ""},
            response.json()["data"],
        )

        # Test single common feature
        response = self.client.post(
            "/v0/features/pivot",
            json={
                "feature_values": [
                    bedr_features.FeaturePivotRequest(feature_name="f2", feature_value="http://blah.com").model_dump()
                ]
            },
        )

        self.assertEqual(200, response.status_code)
        print(f"Expected: {response.json()["data"]}")
        self.assertEqual(
            {
                "feature_value_counts": [
                    {
                        "feature_name": "f2",
                        "feature_description": "generic_description",
                        "values_and_counts": [{"feature_value": "http://blah.com", "entity_count": "2"}],
                    }
                ],
                "incomplete_query": False,
                "reason": "",
            },
            response.json()["data"],
        )
