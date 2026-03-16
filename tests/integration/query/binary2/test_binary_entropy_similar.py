from azul_metastore.query.binary2 import binary_similar
from tests.support import gen
from tests.support import integration_test as etb


def it(x):
    next(x)
    return next(x)


class TestEntitySearch(etb.DynamicTestCase):
    def test_binary_similar_entropy(self):
        series_1 = list(float(x) for x in range(0, 9)) + list(float(x) for x in range(8, 0, -1))
        series_2 = list(float(x) for x in range(0, 9)) + 10 * list(float(x) for x in range(0, 9)) + [8.0] * 5
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
                            "blocks": [x * 0.2 for x in series_2[:7]] + [x * 0.9 for x in series_2[7:]],
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
            ]
        )

        similar_entropies = binary_similar.read_similar_from_entropy(
            ctx=self.writer,
            original_sha256="e1_1",
            entropy=series_1,
            max_matches=10,
            entropy_vector_type="entropy_vector_cosineimil",
        )
        print(similar_entropies)
        self.assertEqual(
            similar_entropies,
            [
                binary_similar.SimilarEntropyMatchRow(sha256="e2_1", score=99.99),
                binary_similar.SimilarEntropyMatchRow(sha256="e3_1", score=99.14),
            ],
        )

        similar_entropies = binary_similar.read_similar_from_entropy(
            ctx=self.writer,
            original_sha256="e5_2",
            entropy=series_2,
            max_matches=10,
            entropy_vector_type="entropy_vector_cosineimil",
        )
        print(similar_entropies)
        self.assertEqual(
            similar_entropies,
            [
                binary_similar.SimilarEntropyMatchRow(sha256="e6_2", score=100.0),
                binary_similar.SimilarEntropyMatchRow(sha256="e8_2", score=97.52),
            ],
        )
