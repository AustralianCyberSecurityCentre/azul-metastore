from azul_metastore.common.tlsh import (
    _swap_byte,
    _tlsh_to_array,
    _unsigned_array_to_signed,
)
from tests.support import unit_test


class TestTLSHEncoding(unit_test.BaseUnitTestCase):
    def test_byte_swap(self):
        self.assertEqual(0x1E, _swap_byte(0xE1))
        self.assertEqual(0xEE, _swap_byte(0xEE))
        self.assertEqual(0xE1, _swap_byte(0x1E))

        self.assertEqual(0x0F, _swap_byte(0xF0))
        self.assertEqual(0xFF, _swap_byte(0xFF))
        self.assertEqual(0xF0, _swap_byte(0x0F))

        self.assertEqual(0x00, _swap_byte(0x00))

    def test_array_conversion(self):
        self.assertEqual([127], _unsigned_array_to_signed([0xFF]))
        self.assertEqual([-128], _unsigned_array_to_signed([0x00]))

        self.assertEqual([-128, -127, -126], _unsigned_array_to_signed([0x00, 0x01, 0x02]))

        self.assertEqual([], _unsigned_array_to_signed([]))

    def test_tlsh_to_array(self):
        # Example TLSH:
        # T17EF08BE2DA3E1DB0419210101161A146BF74E8E729809A987BCE46DC4F6D569C3F5835

        array = _tlsh_to_array("7EF08BE2DA3E1DB0419210101161A146BF74E8E729809A987BCE46DC4F6D569C3F5835")
        array_versioned = _tlsh_to_array("T17EF08BE2DA3E1DB0419210101161A146BF74E8E729809A987BCE46DC4F6D569C3F5835")

        # Version shouldn't impact processing
        self.assertEqual(array, array_versioned)

        self.assertEqual(len(array), 36)
        # Checksum
        self.assertEqual(array[0], 0xE7)
        # LValue
        self.assertEqual(array[1], 0x0F)
        # Q1 Ratio
        self.assertEqual(array[2], 0x8)
        # Q2 Ratio
        self.assertEqual(array[3], 0xB)
        # Data (remainder)
        self.assertEqual(
            array[4:], list(bytearray.fromhex("E2DA3E1DB0419210101161A146BF74E8E729809A987BCE46DC4F6D569C3F5835"))
        )
