from copy import deepcopy
from unittest import TestCase

from .common import BASE_SPEC
from ..content_validators.date_validator import DateValidator


class TestDateValidator(TestCase):
    def test_date_validation_completeness(self):
        validator = DateValidator()
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "XXXX"
        spec["study"]["end_date"] = "XXXX"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 2)

    def test_date_validation_interval(self):
        validator = DateValidator()
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "1.1.2010"
        spec["study"]["end_date"] = "1.1.2009"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

    def test_date_validation_error(self):
        validator = DateValidator()
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "1.22.2010"
        spec["study"]["end_date"] = "1.1.2009"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 2)

    def test_date_validation_multi_errors_month(self):
        validator = DateValidator()
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "1.22.2010"
        spec["study"]["end_date"] = "2.33.2009"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 2)

    def test_date_validation_multi_errors_day(self):
        validator = DateValidator()
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "33.1.2010"
        spec["study"]["end_date"] = "33.2.2009"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 2)

    def test_date_validation_formats(self):
        validator = DateValidator()
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "3.3.2010"
        spec["study"]["end_date"] = "2010-03-04"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 0)

    def test_only_end_date(self):
        validator = DateValidator()
        spec = deepcopy(BASE_SPEC)
        spec["study"]["end_date"] = "2010-03-04"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)
