"""Kiểm tra tham chiếu tên khu dân cư, chỉ chạy trên GitHub."""
import unittest
from pipeline.community_areas import AREAS, community_name, community_label


class CommunityAreaChecks(unittest.TestCase):
    def test_complete_official_reference(self):
        self.assertEqual(set(AREAS), {str(i) for i in range(1, 78)})
        self.assertEqual(len(set(AREAS.values())), 77)

    def test_names_and_codes(self):
        self.assertEqual(community_label(25), "25 — Austin")
        self.assertEqual(community_name("8"), "Near North Side")
        self.assertEqual(community_name("28"), "Near West Side")
        self.assertEqual(community_name("43"), "South Shore")
        self.assertEqual(community_name(59), "McKinley Park")

    def test_unknown_is_kept_explicit(self):
        for value in (None, "Không xác định", 0, 78):
            self.assertEqual(community_label(value), "Không xác định")


if __name__ == "__main__":
    unittest.main(verbosity=2)
