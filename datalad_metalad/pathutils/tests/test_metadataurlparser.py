import unittest
from uuid import UUID

from dataladmetadatamodel.metadatapath import MetadataPath

from ..metadataurlparser import (
    MetadataURLParser,
    TreeMetadataURL,
    UUIDMetadataURL)


class TestMetadataPathParser(unittest.TestCase):
    def test_relative_path(self):
        parser = MetadataURLParser(":a/b/c")
        result = parser.parse()
        self.assertIsInstance(result, TreeMetadataURL)
        self.assertIsNone(result.version)
        self.assertEqual(result.dataset_path, MetadataPath(""))
        self.assertEqual(result.local_path, MetadataPath("a/b/c"))

    def test_tree_version(self):
        parser = MetadataURLParser("tree:/a/b/c@00112233:/x/y")
        result = parser.parse()
        self.assertIsInstance(result, TreeMetadataURL)
        self.assertEqual(result.version, "00112233")
        self.assertEqual(result.dataset_path, MetadataPath("/a/b/c"))
        self.assertEqual(result.local_path, MetadataPath("/x/y"))

    def test_empty_paths(self):
        parser = MetadataURLParser("tree:@00112233")
        result = parser.parse()
        self.assertIsInstance(result, TreeMetadataURL)
        self.assertEqual(result.version, "00112233")
        self.assertEqual(result.dataset_path, MetadataPath(""))
        self.assertEqual(result.local_path, MetadataPath(""))

    def test_uuid(self):
        parser = MetadataURLParser("uuid:00112233-0011-2233-4455-66778899aabb:/a/b/c")
        result = parser.parse()
        self.assertIsInstance(result, UUIDMetadataURL)
        self.assertEqual(result.version, None)
        self.assertEqual(result.uuid, UUID("00112233-0011-2233-4455-66778899aabb"))
        self.assertEqual(result.local_path, MetadataPath("/a/b/c"))

    def test_uuid_relative(self):
        parser = MetadataURLParser("uuid:00112233-0011-2233-4455-66778899aabb:x/b/c")
        result = parser.parse()
        self.assertIsInstance(result, UUIDMetadataURL)
        self.assertEqual(result.version, None)
        self.assertEqual(result.uuid, UUID("00112233-0011-2233-4455-66778899aabb"))
        self.assertEqual(result.local_path, MetadataPath("x/b/c"))

    def test_uuid_empty(self):
        parser = MetadataURLParser("uuid:00112233-0011-2233-4455-66778899aabb")
        result = parser.parse()
        self.assertIsInstance(result, UUIDMetadataURL)
        self.assertEqual(result.version, None)
        self.assertEqual(result.uuid, UUID("00112233-0011-2233-4455-66778899aabb"))
        self.assertEqual(result.local_path, MetadataPath(""))

    def test_uuid_version(self):
        parser = MetadataURLParser("uuid:00112233-0011-2233-4455-66778899aabb@111222:/a/b")
        result = parser.parse()
        self.assertIsInstance(result, UUIDMetadataURL)
        self.assertEqual(result.version, "111222")
        self.assertEqual(result.uuid, UUID("00112233-0011-2233-4455-66778899aabb"))
        self.assertEqual(result.local_path, MetadataPath("/a/b"))

    def test_uuid_version_empty_path(self):
        parser = MetadataURLParser("uuid:00112233-0011-2233-4455-66778899aabb@111222")
        result = parser.parse()
        self.assertIsInstance(result, UUIDMetadataURL)
        self.assertEqual(result.version, "111222")
        self.assertEqual(result.uuid, UUID("00112233-0011-2233-4455-66778899aabb"))
        self.assertEqual(result.local_path, MetadataPath(""))

    def test_blank_path(self):
        parser = MetadataURLParser("a/b/c")
        result = parser.parse()
        self.assertIsInstance(result, TreeMetadataURL)
        self.assertEqual(result.dataset_path, MetadataPath("a/b/c"))
        self.assertEqual(result.local_path, MetadataPath(""))


if __name__ == '__main__':
    unittest.main()
