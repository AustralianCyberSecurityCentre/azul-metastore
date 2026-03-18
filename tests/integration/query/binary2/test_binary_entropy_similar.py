from azul_metastore.common.entropy import ENTROPY_VECTOR_DIMENSION
from azul_metastore.query.binary2 import binary_similar
from tests.support import gen
from tests.support import integration_test as etb


def it(x):
    next(x)
    return next(x)


class TestEntitySearch(etb.DynamicTestCase):
    def test_binary_similar_entropy(self):
        series_1 = 4 * list(float(x) for x in range(0, 9)) + 4 * list(float(x) for x in range(8, 0, -1))
        # series_1 = 2 * list(float(x) for x in range(0, 9)) + 3 * list(float(x) for x in range(8, 1, -1)) + [1]
        series_2 = list(float(x) for x in range(0, 9)) + 10 * list(float(x) for x in range(0, 9)) + [8.0] * 5
        series_3 = [7.7] * 100  # e10_3
        series_4 = [7.9] * 100  # e11_3
        series_5 = [4.0] * 100 + [4.1]  # e12_4
        series_6 = [0.0] * 100  # e13_4
        # Entropy shouldn't be more than 800 and has to be more than 40 to get calculated.
        self.assertGreaterEqual(len(series_1), ENTROPY_VECTOR_DIMENSION)
        self.assertGreaterEqual(len(series_2), ENTROPY_VECTOR_DIMENSION)
        self.assertGreaterEqual(len(series_3), ENTROPY_VECTOR_DIMENSION)
        self.assertGreaterEqual(len(series_4), ENTROPY_VECTOR_DIMENSION)
        self.assertGreaterEqual(len(series_5), ENTROPY_VECTOR_DIMENSION)
        self.assertGreaterEqual(len(series_6), ENTROPY_VECTOR_DIMENSION)
        self.assertLessEqual(len(series_1), 800)
        self.assertLessEqual(len(series_2), 800)

        self.write_binary_events(
            [
                # One series goes range 0->8 then 8->0
                gen.binary_event(
                    eid="e1_1",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": series_1,
                        }
                    },
                ),
                gen.binary_event(
                    eid="e2_1",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": [0.99 * x for x in series_1],
                        }
                    },
                ),
                # These entropies are only slightly out of alignment with e1_1 but don't match at all due to the alignment mismatch.
                gen.binary_event(
                    eid="e22_1",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": series_1[-1:] + series_1[0 : len(series_1) - 1],
                        }
                    },
                ),
                gen.binary_event(
                    eid="e23_1",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": series_1[-20:] + series_1[0 : len(series_1) - 20],
                        }
                    },
                ),
                gen.binary_event(
                    eid="e24_1",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": series_1[-50:] + series_1[0 : len(series_1) - 50],
                        }
                    },
                ),
                gen.binary_event(
                    eid="e3_1",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": [0.9 * x for x in series_1],
                        }
                    },
                ),
                gen.binary_event(
                    eid="e4_1",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": [0.7 * x for x in series_1],
                        }
                    },
                ),
                # Two series goes range 0->8 then 0->8
                gen.binary_event(
                    eid="e5_2",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": series_2,
                        }
                    },
                ),
                gen.binary_event(
                    eid="e6_2",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": series_2,
                        }
                    },
                ),
                gen.binary_event(
                    eid="e7_2",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": [x * 0.2 for x in series_2],
                        }
                    },
                ),
                gen.binary_event(
                    eid="e8_2",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            # As the sequence is very long this has minimal impact on the difference (sequence is 100 digits long)
                            "blocks": [x * 0.2 for x in series_2[:7]] + [x for x in series_2[7:]],
                        }
                    },
                ),
                gen.binary_event(
                    eid="e9_2",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": series_2 * 20,
                        }
                    },
                ),
                # Similar to e11_3(100%) and e7_2 (99%)
                gen.binary_event(
                    eid="e10_3",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": series_3,
                        }
                    },
                ),
                gen.binary_event(
                    eid="e11_3",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": series_4,
                        }
                    },
                ),
                gen.binary_event(
                    eid="e12_4",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": series_5,
                        }
                    },
                ),
                gen.binary_event(
                    eid="e13_4",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": series_6,
                        }
                    },
                ),
            ]
        )
        print("-------------------------------------------------")

        # e1 - a couple of similar entropies multiplied by 0.99 and 0.9 (series 1)
        # Expected to be similar to e2_1, e3_1
        similar_entropies = binary_similar.read_similar_from_entropy(
            ctx=self.writer,
            original_sha256="e1_1",
            entropy=series_1,
            max_matches=10,
        )
        print(similar_entropies)
        self.assertEqual(
            similar_entropies,
            [
                binary_similar.SimilarEntropyMatchRow(sha256="e2_1", score=99.60937500937501),
                binary_similar.SimilarEntropyMatchRow(sha256="e3_1", score=94.7656248725),
            ],
        )

        # e5 - 1 identical and one close (series 2)
        # Expected to be similar to e6_2 and e8_2
        similar_entropies = binary_similar.read_similar_from_entropy(
            ctx=self.writer,
            original_sha256="e5_2",
            entropy=series_2,
            max_matches=10,
        )
        print(similar_entropies)
        self.assertEqual(
            similar_entropies,
            [
                binary_similar.SimilarEntropyMatchRow(sha256="e6_2", score=100.0),
                binary_similar.SimilarEntropyMatchRow(sha256="e8_2", score=97.96874999789063),
            ],
        )

        # e11 - two entropies that are flat and similar (there are two other flat entropies at different magnitudes) (series 3/4)
        # expected to be similar to e11_3
        similar_entropies = binary_similar.read_similar_from_entropy(
            ctx=self.writer,
            original_sha256="e10_3",
            entropy=series_3,
            max_matches=10,
        )
        print(similar_entropies)
        self.assertEqual(
            similar_entropies, [binary_similar.SimilarEntropyMatchRow(sha256="e11_3", score=96.87499988148437)]
        )

        # e12 - two entropies that are flat and similar (there are two other flat entropies at different magnitudes) (series 5/6)
        # expected to be similar to e13_4
        similar_entropies = binary_similar.read_similar_from_entropy(
            ctx=self.writer,
            original_sha256="e12_4",
            entropy=series_5,
            max_matches=10,
        )
        print(similar_entropies)
        self.assertEqual(similar_entropies, [])

        # Allow matches at any level, as this inspects the worst possible match case which should be close to 0%
        # The test verifies a flat entropy of 8.0 matches approximately 0% with an entropy of 0.0
        original = binary_similar.MINIMUM_ENTROPY_SIMILARITY_PERCENTAGE
        binary_similar.MINIMUM_ENTROPY_SIMILARITY_PERCENTAGE = 0
        similar_entropies = binary_similar.read_similar_from_entropy(
            ctx=self.writer,
            original_sha256="e13_4",
            entropy=series_6,
            max_matches=40,
        )
        binary_similar.MINIMUM_ENTROPY_SIMILARITY_PERCENTAGE = original
        self.assertGreater(len(similar_entropies), 3)

        worst_match = similar_entropies[-1]
        # Should be less than 5% of bits matching
        self.assertLessEqual(worst_match.score, 5)
        self.assertEqual(worst_match.sha256, "e10_3")
