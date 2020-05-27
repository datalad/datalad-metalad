from unittest import TestCase

from ..content_validators.date_validator import DateValidator


class TestDateValidator(TestCase):
    def test_date_validation_completeness(self):
        validator = DateValidator()
        spec = {
            "study": {
                "start_date": "d-1",
                "end_date": "d-2",
                "publications": [
                    {
                        "publication": {
                            "title": "t1",
                            "date": "d-3"
                        }
                    },
                    {
                        "publication": {
                            "title": "t2",
                            "date": "d-4"
                        }
                    }
                ]
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 4)

    def test_date_validation_interval(self):
        validator = DateValidator()
        spec = {
            "study": {
                "start_date": "1.1.2010",
                "end_date": "1.1.2009"
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

    def test_partial_study(self):
        validator = DateValidator()
        spec = {
            "study": {
                "publications": [
                    {
                        "publication": {
                            "title": "t1",
                            "date": "xxxx"
                        }
                    }
                ]
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

        spec = {
            "study": {
                "start_date": "1.1.xxx",
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

        spec = {
            "study": {
                "end_date": "1.1.xxx",
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 1)

    def test_minimal_study(self):
        validator = DateValidator()
        spec = {
            "study": {
                "publications": [
                    {
                        "publication": {
                            "title": "t1",
                        }
                    }
                ]
            }
        }
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 0)
