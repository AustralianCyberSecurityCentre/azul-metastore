from azul_metastore.common import fileformat
from tests.support import unit_test


class TestFileformat(unit_test.BaseUnitTestCase):
    def test_get_attachment_type(self):
        # Base case - a text attachment for a file should be viewable
        self.assertEqual(fileformat.get_attachment_type("text/plain", "hash_a", "sub_hash_b"), "text/plain")

        # A text file itself can also be viewed
        self.assertEqual(fileformat.get_attachment_type("text/plain", "hash_a", "hash_a"), "text/plain")

        # A binary file cannot be viewed regardless whether it is the same hash or different
        self.assertEqual(fileformat.get_attachment_type("archive/arj", "hash_a", "hash_a"), None)
        self.assertEqual(fileformat.get_attachment_type("archive/arj", "hash_a", "sub_hash_b"), None)

        # A derived image can be viewed, but not an image file itself
        self.assertEqual(fileformat.get_attachment_type("image/jpg", "hash_a", "hash_a"), None)
        self.assertEqual(fileformat.get_attachment_type("image/jpg", "hash_a", "sub_hash_b"), "image/jpg")
