
from .common import ValidatorTestCase
from ..content_validators.keyword_validator import KeywordValidator


class TestKeywordValidator(ValidatorTestCase):
    def test_no_keywords(self):
        """ Expect one warning """
        spec = {
            "dataset": {
            }
        }
        validator = KeywordValidator("test.yaml")
        errors = validator.perform_validation(spec)
        self.check_warning_and_error_count(errors, 1, 0)

    def test_empty_keywords(self):
        """ Expect one warning """
        spec = {
            "dataset": {
                "keyword": []
            }
        }
        validator = KeywordValidator("test.yaml")
        errors = validator.perform_validation(spec)
        self.check_warning_and_error_count(errors, 1, 0)

    def test_single_keyword_list(self):
        """ Expect np warning """
        spec = {
            "dataset": {
                "keyword": ["a"]
            }
        }
        validator = KeywordValidator("test.yaml")
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 0)

    def test_single_keyword(self):
        """ Expect np warning """
        spec = {
            "dataset": {
                "keyword": "a"
            }
        }
        validator = KeywordValidator("test.yaml")
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 0)
