from azul_metastore.query.binary2 import binary_similar
from tests.support import gen
from tests.support import integration_test as etb


def it(x):
    next(x)
    return next(x)


class TestEntitySearch(etb.DynamicTestCase):

    def test_binary_similar_ssdeep(self):
        hash_same = "192:9p2BHR9woCmKMGRZUCsA7knmxs1yxXMdYKMNbp:j2BHR3DGQ1msyx8M"
        hash_added = (
            "384:p2BHR3DGQknzQnzm5l4nnmsyxlc4vLhkAvLhaAzLZcGvLZ0GvLZ0GvLZ0GvLZcFM:peWnzQnzm5l4nnCckhkshaWZcCZ0CZ0w"
        )
        hash_removed = (
            "96:9pETyYBP1U35TR0gggX7DOspzoMRQRAFyy8i3PNtbRmyFDynmxgPnnIKuKkB9/hD:9p2BuG2sA7knmxs1yxXMdYKMNbp"
        )
        hash_swapped = "192:0R9woCmKMGT1yxYZUCsA7knmxwp2BTMdYKMNbp:0R3DG5yxz1ma2BwM"
        hash_different = "192:9p2BHGxhLAIGvqMo2csjaIjUh+XsDOspzoAMBO:l2PLR3HGF3vdyd1M"

        self.write_binary_events(
            [
                gen.binary_event(eid="same", ssdeep=hash_same),
                gen.binary_event(eid="added", ssdeep=hash_added),
                # check multiple events for same binary
                gen.binary_event(eid="removed", authornv=("1", "1"), ssdeep=hash_removed),
                gen.binary_event(eid="removed", authornv=("2", "1"), ssdeep=hash_removed),
                gen.binary_event(eid="removed", authornv=("3", "1"), ssdeep=hash_removed),
                gen.binary_event(eid="swapped", ssdeep=hash_swapped),
                gen.binary_event(eid="different", ssdeep=hash_different),
            ]
        )

        # check for exclusion of hash_same and hash_different
        hashScores = binary_similar.read_similar_from_ssdeep(ctx=self.writer, fuzzyHash=hash_same, maxCount=100)
        self.assertEqual(
            hashScores,
            [
                {"sha256": "removed", "score": 79},
                {"sha256": "swapped", "score": 72},
                {"sha256": "added", "score": 33},
            ],
        )

        # check that maxCount is behaving correctly
        hashScores = binary_similar.read_similar_from_ssdeep(ctx=self.writer, fuzzyHash=hash_same, maxCount=1)
        self.assertEqual(hashScores, [{"sha256": "removed", "score": 79}])

    def test_binary_similar_tlsh(self):
        hash_base = "T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        hash_mod_a = "T1BBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        hash_mod_b = "T1CCCCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

        self.write_binary_events(
            [
                gen.binary_event(eid="same", tlsh=hash_base),
                gen.binary_event(eid="mod_a", tlsh=hash_mod_a),
                gen.binary_event(eid="mod_b", tlsh=hash_mod_b),
            ]
        )

        # check for exclusion of hash_same and hash_different
        hashScores = binary_similar.read_similar_from_tlsh(ctx=self.writer, tlsh=hash_base, maxCount=100)
        self.assertEqual(
            hashScores,
            [
                {"sha256": "mod_a", "score": 99.92},
                {"sha256": "mod_b", "score": 99.42},
            ],
        )

        # check that maxCount is behaving correctly
        hashScores = binary_similar.read_similar_from_tlsh(ctx=self.writer, tlsh=hash_base, maxCount=1)
        self.assertEqual(hashScores, [{"sha256": "mod_a", "score": 99.92}])

    def test_read_similar_from_features(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1", authornv=("a1", "1"), authorsec=gen.g1_1, fvl=[("f1", f"v{x}") for x in range(10)]
                ),
                gen.binary_event(
                    eid="e1", authornv=("a2", "1"), authorsec=gen.g2_1, fvl=[("f2", f"v{x}") for x in range(10)]
                ),
                gen.binary_event(
                    eid="e2", authornv=("a1", "1"), authorsec=gen.g1_1, fvl=[("f1", f"v{x}") for x in range(10)]
                ),
                gen.binary_event(
                    eid="e2", authornv=("a2", "1"), authorsec=gen.g2_1, fvl=[("f2", f"v{x}") for x in range(10)]
                ),
                gen.binary_event(
                    eid="e3", authornv=("a1", "1"), authorsec=gen.g2_1, fvl=[("f1", f"v{x}") for x in range(9)]
                ),
                gen.binary_event(
                    eid="e4", authornv=("a2", "1"), authorsec=gen.g3_1, fvl=[("f2", f"v{x}") for x in range(8)]
                ),
                gen.binary_event(
                    eid="e5", authornv=("a1", "1"), sourcesec=gen.g3_1, fvl=[("f1", f"v{x}") for x in range(5)]
                ),
                gen.binary_event(
                    eid="e5", authornv=("a2", "1"), sourcesec=gen.g3_1, fvl=[("f2", f"v{x}") for x in range(5)]
                ),
            ]
        )

        results = it(binary_similar.read_similar_from_features(ctx=self.writer, sha256="e1"))
        self.assertEqual(4, len(results["matches"]))

        self.assertEqual("e2", results["matches"][0]["sha256"])
        self.assertEqual("e5", results["matches"][1]["sha256"])
        self.assertEqual("e3", results["matches"][2]["sha256"])
        self.assertEqual("e4", results["matches"][3]["sha256"])
        self.assertEqual(100, results["matches"][0]["score_percent"])
        self.assertEqual(50, results["matches"][1]["score_percent"])
        self.assertEqual(45, results["matches"][2]["score_percent"])
        self.assertEqual(40, results["matches"][3]["score_percent"])

        results = it(binary_similar.read_similar_from_features(ctx=self.es1, sha256="e1"))
        self.assertEqual(1, len(results["matches"]))

        self.assertEqual("e2", results["matches"][0]["sha256"])

        results = it(binary_similar.read_similar_from_features(ctx=self.es2, sha256="e1"))
        self.assertEqual(2, len(results["matches"]))

        self.assertEqual("e2", results["matches"][0]["sha256"])
        self.assertEqual("e3", results["matches"][1]["sha256"])

        results = it(binary_similar.read_similar_from_features(ctx=self.es3, sha256="e1"))
        self.assertEqual(4, len(results["matches"]))

        self.assertEqual("e2", results["matches"][0]["sha256"])
        self.assertEqual("e5", results["matches"][1]["sha256"])
        self.assertEqual("e3", results["matches"][2]["sha256"])
        self.assertEqual("e4", results["matches"][3]["sha256"])

        # check cache
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e5", authornv=("a2", "2"), sourcesec=gen.g3_1, fvl=[("f2", f"v{x}") for x in range(10)]
                ),
            ]
        )
        # check reads old results
        results = it(binary_similar.read_similar_from_features(ctx=self.writer, sha256="e1"))
        self.assertEqual(4, len(results["matches"]))
        self.assertEqual("e2", results["matches"][0]["sha256"])
        self.assertEqual("e5", results["matches"][1]["sha256"])
        self.assertEqual("e3", results["matches"][2]["sha256"])
        self.assertEqual("e4", results["matches"][3]["sha256"])
        self.assertEqual(100, results["matches"][0]["score_percent"])
        self.assertEqual(50, results["matches"][1]["score_percent"])
        self.assertEqual(45, results["matches"][2]["score_percent"])
        self.assertEqual(40, results["matches"][3]["score_percent"])
        # check recalculates
        results = it(binary_similar.read_similar_from_features(ctx=self.writer, sha256="e1", recalculate=True))
        self.assertEqual(4, len(results["matches"]))
        self.assertEqual("e2", results["matches"][0]["sha256"])
        self.assertEqual("e5", results["matches"][1]["sha256"])
        self.assertEqual("e3", results["matches"][2]["sha256"])
        self.assertEqual("e4", results["matches"][3]["sha256"])
        self.assertEqual(100, results["matches"][0]["score_percent"])
        self.assertEqual(75, results["matches"][1]["score_percent"])
        self.assertEqual(45, results["matches"][2]["score_percent"])
        self.assertEqual(40, results["matches"][3]["score_percent"])
        # check reads new results
        results = it(binary_similar.read_similar_from_features(ctx=self.writer, sha256="e1"))
        self.assertEqual(4, len(results["matches"]))
        self.assertEqual("e2", results["matches"][0]["sha256"])
        self.assertEqual("e5", results["matches"][1]["sha256"])
        self.assertEqual("e3", results["matches"][2]["sha256"])
        self.assertEqual("e4", results["matches"][3]["sha256"])
        self.assertEqual(100, results["matches"][0]["score_percent"])
        self.assertEqual(75, results["matches"][1]["score_percent"])
        self.assertEqual(45, results["matches"][2]["score_percent"])
        self.assertEqual(40, results["matches"][3]["score_percent"])
