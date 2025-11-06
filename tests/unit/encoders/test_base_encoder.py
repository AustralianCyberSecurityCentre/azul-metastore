from azul_metastore.common import utils
from azul_metastore.encoders.base_encoder import BaseIndexEncoder
from tests.support import unit_test


class TestEncoder(unit_test.BaseUnitTestCase):
    def test_sec_encoding(self):
        # test single with the default value
        req = {"security": utils.azsec().get_default_security()}
        BaseIndexEncoder._encode_security(req)
        self.assertEqual(
            req["encoded_security"],
            {"inclusive": ["s-any"], "exclusive": ["s-low"], "markings": ["s-any"], "num_exclusive": 1},
        )

        # test single with exclusive
        req = {"security": "LOW"}
        BaseIndexEncoder._encode_security(req)
        self.assertEqual(
            req["encoded_security"],
            {"inclusive": ["s-any"], "exclusive": ["s-low"], "markings": ["s-any"], "num_exclusive": 1},
        )

        # test single with exclusive and markings
        req = {"security": "LOW TLP:CLEAR"}
        BaseIndexEncoder._encode_security(req)
        self.assertEqual(
            req["encoded_security"],
            {"inclusive": ["s-any"], "exclusive": ["s-low"], "markings": ["s-tlp-clear"], "num_exclusive": 1},
        )

        # test empty
        req = {}
        BaseIndexEncoder._encode_security(req)
        self.assertEqual(
            req["encoded_security"],
            {"inclusive": ["s-any"], "exclusive": ["s-low"], "markings": ["s-any"], "num_exclusive": 1},
        )
        req = {"security": ""}
        BaseIndexEncoder._encode_security(req)
        self.assertEqual(
            req["encoded_security"],
            {"inclusive": ["s-any"], "exclusive": ["s-low"], "markings": ["s-any"], "num_exclusive": 1},
        )

        # test wrong types
        self.assertRaises(AttributeError, BaseIndexEncoder._encode_security, "TLP:CLEAR")
