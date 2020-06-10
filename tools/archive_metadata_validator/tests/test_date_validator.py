from unittest import TestCase

from .common import BASE_SPEC
from ..content_validators.date_validator import DateValidator


class TestDateValidator(TestCase):
    def test_date_validation_completeness(self):
        validator = DateValidator()
        spec = {**BASE_SPEC}
        spec["study"]["start_date"] = "XXXX"
        spec["study"]["end_date"] = "XXXX"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 2)

    def test_date_validation_interval(self):
        validator = DateValidator()
        spec = {**BASE_SPEC}
        spec["study"]["start_date"] = "1.1.2010"
        spec["study"]["end_date"] = "1.1.2009"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)
