from copy import deepcopy

from .common import BASE_SPEC, MINIMAL_SPEC, ValidatorTestCase
from ..content_validators.reference_validator import ReferenceValidator


class TestReferenceValidator(ValidatorTestCase):
    def test_perform_validation_publication_single(self):
        spec = deepcopy(BASE_SPEC)
        spec["publication"][0]["author"] = "a@example.com"
        validator = ReferenceValidator("test.yaml", BASE_SPEC)
        validator.perform_validation()

    def test_perform_validation_publication_list(self):
        validator = ReferenceValidator("test.yaml", BASE_SPEC)
        validator.perform_validation()

    def test_fail_validation_pi(self):
        spec = deepcopy(BASE_SPEC)
        spec["study"]["principal_investigator"] = "x@example.com"
        validator = ReferenceValidator("test.yaml", spec)
        errors = validator.perform_validation()
        self.check_warning_and_error_count(errors, 0, 1)

    def test_fail_validation_publication_single(self):
        spec = deepcopy(BASE_SPEC)
        spec["publication"][0]["author"] = "x@example.com"
        validator = ReferenceValidator("test.yaml", spec)
        errors = validator.perform_validation()
        self.check_warning_and_error_count(errors, 0, 2)

    def test_fail_validation_publication_list(self):
        spec = deepcopy(BASE_SPEC)
        spec["publication"][0]["author"] = [
            "x@example.com",
            "y@example.com",
            "z@example.com"
        ]
        validator = ReferenceValidator("test.yaml", spec)
        errors = validator.perform_validation()
        self.check_warning_and_error_count(errors, 0, 4)

    def test_corresponding_author_validation(self):
        spec = deepcopy(BASE_SPEC)
        spec["publication"][0]["corresponding_author"] = "x@example.com"
        validator = ReferenceValidator("test.yaml", spec)
        errors = validator.perform_validation()
        self.check_warning_and_error_count(errors, 0, 1)

    def test_author_validation(self):
        spec = deepcopy(BASE_SPEC)
        spec["publication"][0]["author"] = "x@example.com"
        spec["publication"][0]["corresponding_author"] = "x@example.com"
        validator = ReferenceValidator("test.yaml", spec)
        errors = validator.perform_validation()
        self.check_warning_and_error_count(errors, 0, 1)

    def test_author_corresponding_author_validation(self):
        spec = deepcopy(BASE_SPEC)
        spec["publication"][0]["author"] = "x@example.com"
        spec["publication"][0]["corresponding_author"] = "y@example.com"
        validator = ReferenceValidator("test.yaml", spec)
        errors = validator.perform_validation()
        self.check_warning_and_error_count(errors, 0, 2)

    def test_study_contributor_fail_validation(self):
        spec = deepcopy(BASE_SPEC)
        spec["study"]["contributor"] = "x@example.com"
        validator = ReferenceValidator("test.yaml", spec)
        errors = validator.perform_validation()
        self.check_warning_and_error_count(errors, 0, 1)

    def test_minimal_study(self):
        validator = ReferenceValidator("test.yaml", MINIMAL_SPEC)
        errors = validator.perform_validation()
        self.check_warning_and_error_count(errors, 0, 0)
