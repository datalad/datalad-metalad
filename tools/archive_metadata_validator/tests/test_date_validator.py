from copy import deepcopy

from .common import BASE_SPEC, ValidatorTestCase
from ..content_validators.date_validator import DateValidator


class TestDateValidator(ValidatorTestCase):
    def test_date_validation_completeness(self):
        validator = DateValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "XXXX"
        spec["study"]["end_date"] = "XXXX"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 2)

    def test_date_validation_interval(self):
        validator = DateValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "1.1.2010"
        spec["study"]["end_date"] = "1.1.2009"
        errors = validator.perform_validation(spec)
        self.check_warning_and_error_count(errors, 0, 1)

    def test_date_validation_error(self):
        validator = DateValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "1.22.2010"
        spec["study"]["end_date"] = "1.1.2009"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 2)

    def test_date_validation_multi_errors_month(self):
        validator = DateValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "1.22.2010"
        spec["study"]["end_date"] = "2.33.2009"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 2)

    def test_date_validation_multi_errors_day(self):
        validator = DateValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "33.1.2010"
        spec["study"]["end_date"] = "33.2.2009"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 2)

    def test_date_validation_formats(self):
        validator = DateValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "3.3.2010"
        spec["study"]["end_date"] = "2010-03-04"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 0)

    def test_start_date_missing(self):
        validator = DateValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["end_date"] = "2010-03-04"
        errors = validator.perform_validation(spec)
        self.check_warning_and_error_count(errors, 0, 1)

    def test_publication_good_year(self):
        """ Expect one warning that """
        validator = DateValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "1.1.2000"
        spec["study"]["end_date"] = "31.12.2019"
        spec["publication"] = [{"title": "t", "year": 2010}]
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 0)

    def test_publication_year_early(self):
        validator = DateValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "1.1.2000"
        spec["study"]["end_date"] = "31.12.2019"
        spec["publication"] = [{"title": "t", "year": 1990}]
        errors = validator.perform_validation(spec)
        self.check_warning_and_error_count(errors, 1, 0)

    def test_publication_year_late(self):
        """ Expect a warning that the publication date is after the end date """
        validator = DateValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "1.1.2000"
        spec["study"]["end_date"] = "31.12.2019"
        spec["publication"] = [{"title": "t", "year": 2020}]
        errors = validator.perform_validation(spec)
        self.check_warning_and_error_count(errors, 1, 0)

    def test_publication_year_future(self):
        """ Expect a warning that the publication date is after the end date """
        validator = DateValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "1.1.2000"
        spec["study"]["end_date"] = "31.12.3019"
        spec["publication"] = [{"title": "t", "year": 3000}]
        errors = validator.perform_validation(spec)
        self.check_warning_and_error_count(errors, 2, 0)

    def test_start_date_future(self):
        validator = DateValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "1.1.3000"
        spec["study"]["end_date"] = "1.1.4000"
        errors = validator.perform_validation(spec)
        self.check_warning_and_error_count(errors, 1, 1)

    def test_end_date_future(self):
        validator = DateValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["start_date"] = "1.1.2000"
        spec["study"]["end_date"] = "1.1.4000"
        errors = validator.perform_validation(spec)
        self.check_warning_and_error_count(errors, 1, 0)
