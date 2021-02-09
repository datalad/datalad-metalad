import unittest

from datalad_metalad.pathutils.metadatapathparser import MetadataPathParser, \
    TreeMetadataPath, UUIDMetadataPath


class TestMetadataPathParser(unittest.TestCase):
    def test_relative_path(self):
        parser = MetadataPathParser(":a/b/c")
        result = parser.parse()
        self.assertIsInstance(result, TreeMetadataPath)
        self.assertIsNone(result.version)
        self.assertEqual(result.local_path, "a/b/c")

    def test_tree_version(self):
        parser = MetadataPathParser("tree:/a/b/c@00112233:/x/y")
        result = parser.parse()
        self.assertIsInstance(result, TreeMetadataPath)
        self.assertEqual(result.version, "00112233")
        self.assertEqual(result.dataset_path, "/a/b/c")
        self.assertEqual(result.local_path, "/x/y")

    def test_empty_paths(self):
        parser = MetadataPathParser("tree:@00112233")
        result = parser.parse()
        self.assertIsInstance(result, TreeMetadataPath)
        self.assertEqual(result.version, "00112233")
        self.assertEqual(result.dataset_path, "")
        self.assertEqual(result.local_path, "")

    def test_uuid(self):
        parser = MetadataPathParser("uuid:00112233-0011-2233-4455-66778899aabb:/a/b/c")
        result = parser.parse()
        self.assertIsInstance(result, UUIDMetadataPath)
        self.assertEqual(result.version, None)
        self.assertEqual(result.uuid, "00112233-0011-2233-4455-66778899aabb")
        self.assertEqual(result.local_path, "/a/b/c")

    def test_uuid_relative(self):
        parser = MetadataPathParser("uuid:00112233-0011-2233-4455-66778899aabb:x/b/c")
        result = parser.parse()
        self.assertIsInstance(result, UUIDMetadataPath)
        self.assertEqual(result.version, None)
        self.assertEqual(result.uuid, "00112233-0011-2233-4455-66778899aabb")
        self.assertEqual(result.local_path, "x/b/c")

    def test_uuid_empty(self):
        parser = MetadataPathParser("uuid:00112233-0011-2233-4455-66778899aabb")
        result = parser.parse()
        self.assertIsInstance(result, UUIDMetadataPath)
        self.assertEqual(result.version, None)
        self.assertEqual(result.uuid, "00112233-0011-2233-4455-66778899aabb")
        self.assertEqual(result.local_path, "")

    def test_uuid_version(self):
        parser = MetadataPathParser("uuid:00112233-0011-2233-4455-66778899aabb@111222:/a/b")
        result = parser.parse()
        self.assertIsInstance(result, UUIDMetadataPath)
        self.assertEqual(result.version, "111222")
        self.assertEqual(result.uuid, "00112233-0011-2233-4455-66778899aabb")
        self.assertEqual(result.local_path, "/a/b")

    def test_uuid_version_empty_path(self):
        parser = MetadataPathParser("uuid:00112233-0011-2233-4455-66778899aabb@111222")
        result = parser.parse()
        self.assertIsInstance(result, UUIDMetadataPath)
        self.assertEqual(result.version, "111222")
        self.assertEqual(result.uuid, "00112233-0011-2233-4455-66778899aabb")
        self.assertEqual(result.local_path, "")


if __name__ == '__main__':
    unittest.main()
