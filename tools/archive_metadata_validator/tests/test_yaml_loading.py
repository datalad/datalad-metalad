from pathlib import PosixPath
from unittest import TestCase
from tempfile import NamedTemporaryFile

from ..validator import SpecValidator


SCHEMA = """
$schema: "http://json-schema.org/draft-07/schema#"
$id: "http://psychoinformatics.inm7.de/schemas/archived_study.json"
"""


class TestYAMLLoading(TestCase):
    def test_no_iso_date_conversion(self):
        spec = SpecValidator._load_yaml_string("date: 2000-01-01")
        self.assertIsInstance(spec["date"], str)

    def test_mapping_error(self):
        with NamedTemporaryFile() as schema_file:
            schema_file.write(SCHEMA.encode())
            schema_file.seek(0)
            validator = SpecValidator(PosixPath(schema_file.name), [])
            spec = validator.load_yaml_string("date: x: 2000-01-01")
            self.assertIsNone(spec)
            self.assertEqual(len(validator.errors), 1)
            self.assertTrue(validator.errors[0].startswith("YAML error: line: 1: column: 8: mapping"))
