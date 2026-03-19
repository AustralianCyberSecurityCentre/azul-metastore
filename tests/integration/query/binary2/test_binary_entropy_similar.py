from azul_metastore.common.entropy import ENTROPY_VECTOR_DIMENSION
from azul_metastore.query.binary2 import binary_similar
from tests.support import gen
from tests.support import integration_test as etb
from azul_bedrock.models_restapi import binaries as bedr_binaries


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
        # 000486aaac9dd21e88b3dc65fd854dd83519b1fbcc224a70530bc3ec8cbd1a5d
        series_7 = [
            4.60038405251792,
            2.154752993529115,
            2.2790719140485143,
            0,
            5.289625923620084,
            5.031774269270483,
            5.577961786951351,
            4.975646702568736,
            5.081319029590194,
            4.984935050873684,
            4.9397671408408765,
            4.473714748622051,
            4.738427999899732,
            5.243297248673907,
            5.492075824970679,
            4.382973457951267,
            4.359339696817719,
            5.14965250271855,
            5.2420303247524505,
            5.526141292833417,
            5.405179877549828,
            5.230136815937413,
            5.211118212935588,
            5.146448421201723,
            5.422156330870965,
            5.228410013936286,
            6.01717466206671,
            5.023354376348963,
            5.35257146770936,
            5.7247417649741275,
            5.03410054886346,
            4.434924380504992,
            1.720721562484049,
            2.064006156129111,
            3.475284742435969,
            3.2052701126531464,
            3.0599564330885793,
            3.213636069346501,
            4.159647359961735,
            4.392845257206208,
            4.059873720817251,
            2.8451830504629436,
            0.7717234671756242,
            4.079972660142236,
            3.997477985975108,
            3.8374114491664333,
            3.6674066718692533,
            4.002821446523798,
            4.044965686574988,
            3.505984132584778,
            2.8988762749065904,
            1.747497941281992,
            4.080037680895124,
            4.75739761765261,
            4.915698448273893,
            4.591885571019407,
            4.880960996787614,
            3.211104041659037,
            2.136130176318906,
            1.4810703322390562,
            0,
            0,
            3.612272594191376,
            3.631516879766229,
            2.3229630887303037,
            0,
            3.6528979013415945,
            4.723600202305234,
            5.1650060402063565,
            5.168920432665744,
            4.862910821773122,
            5.149376436041621,
            3.234432650718684,
            3.301265872226776,
            3.3855512476289498,
            2.2047117934348766,
            2.127244144788556,
            0,
            5.822912132077695,
            5.754400818676115,
            7.109950634588154,
            5.352476474085914,
            6.526563671013815,
            7.0745815568049775,
            5.454380605467396,
            6.999896340091056,
            7.149266773518898,
            6.458900681551046,
            5.48739236431,
            7.140810535095538,
            5.816782513530299,
            6.882471475041048,
            7.233035693198806,
            6.5292239001551815,
            5.745785526630069,
            7.017589015923013,
            6.436531224125109,
            5.481758373806121,
            7.124151697179559,
            6.012724522332896,
            6.847086123377135,
            6.597622796450676,
            5.163349121779959,
            6.970206738061523,
            6.5351510393611525,
            6.184800581136501,
        ]

        # f677017665d7b9931407f952bdec0fd43ef526cb1645e64401cbbb29af05d128
        series_8 = [
            4.720450499323176,
            2.15846162093121,
            2.2338833989987847,
            0.07372691234324169,
            5.321384991171948,
            4.902406242498934,
            5.256176617503601,
            5.398127527987158,
            5.149542661041837,
            4.748096154327834,
            4.7004083387607,
            4.713581376592336,
            4.757774923906266,
            4.694035676819925,
            5.378772621769213,
            5.252784653767986,
            5.640938444023559,
            5.582447140824356,
            4.8546807342396265,
            5.239768504446074,
            5.130339636103362,
            5.424126928943236,
            5.63408361934605,
            5.784610661580059,
            5.041853702804737,
            5.871400708945753,
            5.303004028255016,
            4.705157851383219,
            2.6243559211964933,
            0,
            1.7010943597000026,
            1.9687346295931463,
            3.4790731835084463,
            4.448613055841518,
            5.179556830306563,
            4.80083533766917,
            4.795440940318294,
            4.85112279307637,
            4.745655626332129,
            2.429024178286725,
            0.9102091092643714,
            4.46744493706729,
            3.6770136131000486,
            3.7169788455228474,
            3.259581499014937,
            3.582725566280547,
            3.954812854117942,
            3.648918749940104,
            2.730618655702134,
            1.6240510467538796,
            3.5241698173013343,
            5.090830050350368,
            5.048511311982382,
            4.634044830218021,
            4.797210536073154,
            1.3041948235616236,
            2.027061744179574,
            1.05045024215147,
            3.447503416533365,
            3.61857097089878,
            3.135993101674889,
            0,
            3.6528979013415945,
            4.723600202305234,
            5.1650060402063565,
            5.168920432665744,
            4.862910821773122,
            5.149376436041621,
            3.242245150718684,
            3.301265872226776,
            3.39388108588083,
            2.0389923355155135,
            2.2820016965829857,
            0,
            5.845779240868199,
            5.754400818676115,
            7.109950634588154,
            5.352476474085914,
            6.526563671013815,
            7.0745815568049775,
            5.454380605467396,
            6.999896340091056,
            7.149266773518898,
            6.458900681551046,
            5.48739236431,
            7.140810535095538,
            5.827543792835625,
            6.7895016740262815,
            7.262370751809458,
            6.478891716211422,
            5.773170750598462,
            7.017589015923013,
            6.432057602923051,
            5.471878197974164,
            7.134912976484885,
            6.009385643534952,
            6.847086123377135,
            6.5851335285875034,
            5.1678227429820165,
            6.98096801736685,
            6.5429635393611525,
            6.190428451574378,
        ]

        # 9ecec72c5fe3c83c122043cad8ceb80d239d99d03b8ea665490bbced183ce42a
        series_9 = [
            4.697821453036379,
            2.116613794672628,
            2.2472683615091538,
            0,
            5.188117479648983,
            5.352027470700375,
            5.09901822423536,
            5.649242592844835,
            5.602054131521654,
            4.764775864136479,
            5.260948402494107,
            5.22068327647017,
            5.3694548742279435,
            5.530331274307594,
            5.904063862761619,
            4.997289218912181,
            5.747133082987906,
            5.374763203378694,
            5.344958242485804,
            5.19006791978372,
            5.361784717323748,
            5.0484011048502815,
            5.561895951622197,
            5.427618146420842,
            5.256079030710875,
            5.246366240383002,
            5.1629818379013654,
            1.360572323833987,
            1.7283706094373052,
            1.848344226707147,
            3.0450105610927607,
            1.7489938790726636,
            2.129284388643184,
            4.527694752501947,
            4.711580448228297,
            4.45862677330346,
            4.457991659158707,
            4.621458771020782,
            4.651065857762042,
            2.5010078530557833,
            4.281557452101222,
            4.074972156621019,
            3.8597360691151934,
            3.7168517481464627,
            3.4904067236634635,
            4.101442774716176,
            3.794337606777507,
            3.9661805143944058,
            1.719858532249088,
            1.8279205838692416,
            4.473351226724396,
            5.053153954974075,
            5.145190359817295,
            5.027712455838734,
            4.708150707063306,
            4.748828239421687,
            1.394771103139313,
            0,
            1.9981696708413161,
            1.8839004477222554,
            1.955600167901777,
            0.7121844478410647,
            3.6375617055401923,
            3.58317425579021,
            2.822981191716793,
            0,
            3.6528979013415945,
            4.723600202305234,
            5.1650060402063565,
            5.168920432665744,
            4.862910821773122,
            5.149376436041621,
            3.242245150718684,
            3.301265872226776,
            3.415476911654225,
            1.9840888119299218,
            3.4032607630037086,
            0,
            5.740159500482794,
            5.754400818676115,
            7.109950634588154,
            5.352476474085914,
            6.526563671013815,
            7.0745815568049775,
            5.454380605467396,
            6.999896340091056,
            7.149266773518898,
            6.458900681551046,
            5.48739236431,
            7.140810535095538,
            5.816782513530299,
            6.951207303189845,
            7.235984472504132,
            6.489792962509542,
            5.795246861748474,
            7.017589015923013,
            6.436531224125109,
            5.481758373806121,
            7.124151697179559,
            6.012724522332896,
            6.847086123377135,
            6.597622796450676,
            5.163349121779959,
            6.970206738061523,
            6.5351510393611525,
            6.221405305811927,
        ]

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
                gen.binary_event(
                    eid="000486aaac9dd21e88b3dc65fd854dd83519b1fbcc224a70530bc3ec8cbd1a5d",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": series_7,
                        }
                    },
                ),
                gen.binary_event(
                    eid="f677017665d7b9931407f952bdec0fd43ef526cb1645e64401cbbb29af05d128",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": series_8,
                        }
                    },
                ),
                gen.binary_event(
                    eid="9ecec72c5fe3c83c122043cad8ceb80d239d99d03b8ea665490bbced183ce42a",
                    authornv=("entropy", "1"),
                    info={
                        "entropy": {
                            "idk": True,
                            "blocks": series_9,
                        }
                    },
                ),
            ]
        )
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
                bedr_binaries.SimilarEntropyMatchRow(sha256="e2_1", score=99.6094),
                bedr_binaries.SimilarEntropyMatchRow(sha256="e3_1", score=94.7656),
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
                bedr_binaries.SimilarEntropyMatchRow(sha256="e6_2", score=100.0),
                bedr_binaries.SimilarEntropyMatchRow(sha256="e8_2", score=97.9687),
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
        self.assertEqual(similar_entropies, [bedr_binaries.SimilarEntropyMatchRow(sha256="e11_3", score=96.875)])

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
        print(similar_entropies)
        binary_similar.MINIMUM_ENTROPY_SIMILARITY_PERCENTAGE = original
        self.assertGreater(len(similar_entropies), 3)

        worst_match = similar_entropies[-1]
        # Should be less than 5% of bits matching
        self.assertLessEqual(worst_match.score, 5)
        self.assertEqual(worst_match.sha256, "e10_3")

        # Allow matches at any level, as this inspects the worst possible match case which should be close to 0%
        # The test verifies a flat entropy of 8.0 matches approximately 0% with an entropy of 0.0
        similar_entropies = binary_similar.read_similar_from_entropy(
            ctx=self.writer,
            original_sha256="000486aaac9dd21e88b3dc65fd854dd83519b1fbcc224a70530bc3ec8cbd1a5d",
            entropy=series_7,
            max_matches=20,
        )
        print(similar_entropies)
        self.assertEqual(
            similar_entropies,
            [
                bedr_binaries.SimilarEntropyMatchRow(
                    sha256="f677017665d7b9931407f952bdec0fd43ef526cb1645e64401cbbb29af05d128", score=93.4375
                ),
                bedr_binaries.SimilarEntropyMatchRow(
                    sha256="9ecec72c5fe3c83c122043cad8ceb80d239d99d03b8ea665490bbced183ce42a", score=92.5781
                ),
            ],
        )
