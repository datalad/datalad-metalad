from unittest import TestCase

from ..content_validators.reference_validator import ReferenceValidator


PERSONS = [
    {
        "person": {
            "id": "id-1",
            "first_name": "fn-1",
            "last_name": "ln-1"
        }
    },
    {
        "person": {
            "id": "id-2",
            "first_name": "fn-2",
            "last_name": "ln-2"
        }
    }
]


class TestReferenceValidator(TestCase):
    def test_perform_validation_id(self):
        validator = ReferenceValidator()
        spec = {
            "study": {
                "persons": PERSONS,
                "contact_point": "id-2",
            }
        }
        validator.perform_validation(spec)

    def test_perform_validation_names(self):
        validator = ReferenceValidator()
        spec = {
            "study": {
                "persons": PERSONS,
                "contact_point": {
                    "first_name": "fn-2",
                    "last_name": "ln-2"
                }
            }
        }
        validator.perform_validation(spec)

    def test_fail_validation_ids(self):
        validator = ReferenceValidator()
        spec = {
            "study": {
                "persons": PERSONS,
                "contact_point": "id-3",
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

    def test_fail_validation_names(self):
        validator = ReferenceValidator()
        spec = {
            "study": {
                "persons": PERSONS,
                "contact_point": {
                    "first_name": "fn-x",
                    "last_name": "ln-2"
                }
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

        spec = {
            "study": {
                "persons": PERSONS,
                "contact_point": {
                    "first_name": "fn-2",
                    "last_name": "ln-x"
                }
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

    def test_contact_point_validation(self):
        validator = ReferenceValidator()
        spec = {
            "study": {
                "persons": PERSONS,
                "contact_point": "id-3",
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

    def test_dataset_contact_point_validation(self):
        validator = ReferenceValidator()
        spec = {
            "study": {
                "persons": PERSONS,
                "dataset": {
                    "contact_point": "id-4"
                }
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

    def test_author_validation(self):
        validator = ReferenceValidator()
        spec = {
            "study": {
                "persons": PERSONS,
                "publications": [
                    {
                        "publication": {
                            "authors": [
                                "id-5"
                            ]
                        }
                    }
                ]
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

        spec = {
            "study": {
                "persons": PERSONS,
                "publications": [
                    {
                        "publication": {
                            "authors": [
                                "id-1",
                                "id-6"
                            ]
                        }
                    }
                ]
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

    def test_minimal_study(self):
        validator = ReferenceValidator()
        spec = {
            "study": {
                "name": "n-1",
                "dataset": {
                    "name": "dn-1",
                    "url": "url://example.com/dataset"
                }
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 0)

    def test_empty_persons_study(self):
        validator = ReferenceValidator()
        spec = {
            "study": {
                "name": "n-1",
                "dataset": {
                    "name": "dn-1",
                    "url": "url://example.com/dataset"
                },
                "contact_point": "id-1"
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)
