from copy import deepcopy
from unittest import TestCase

from .common import BASE_SPEC, MINIMAL_SPEC
from ..content_validators.reference_validator import ReferenceValidator


class TestReferenceValidator(TestCase):
    def test_perform_validation_publication_single(self):
        validator = ReferenceValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["publication"][0]["author"] = "a@example.com"
        validator.perform_validation(BASE_SPEC)

    def test_perform_validation_publication_list(self):
        validator = ReferenceValidator("test.yaml")
        validator.perform_validation(BASE_SPEC)

    def test_fail_validation_pi(self):
        validator = ReferenceValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["principal_investigator"] = "x@example.com"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

    def test_fail_validation_publication_single(self):
        validator = ReferenceValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["publication"][0]["author"] = "x@example.com"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 2)

    def test_fail_validation_publication_list(self):
        validator = ReferenceValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["publication"][0]["author"] = [
            "x@example.com",
            "y@example.com",
            "z@example.com"
        ]
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 4)

    def test_corresponding_author_validation(self):
        validator = ReferenceValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["publication"][0]["corresponding_author"] = "x@example.com"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

    def test_author_validation(self):
        validator = ReferenceValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["publication"][0]["author"] = "x@example.com"
        spec["publication"][0]["corresponding_author"] = "x@example.com"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

    def test_author_corresponding_author_validation(self):
        validator = ReferenceValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["publication"][0]["author"] = "x@example.com"
        spec["publication"][0]["corresponding_author"] = "y@example.com"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 2)

    def test_study_contributor_fail_validation(self):
        validator = ReferenceValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        spec["study"]["contributor"] = "x@example.com"
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

    def test_minimal_study(self):
        validator = ReferenceValidator("test.yaml")
        errors = validator.perform_validation(MINIMAL_SPEC)
        self.assertEqual(len(errors), 0)
