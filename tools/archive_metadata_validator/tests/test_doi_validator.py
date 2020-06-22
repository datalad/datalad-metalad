from copy import deepcopy
from unittest.mock import Mock, call, patch

from .common import BASE_SPEC, ValidatorTestCase
from ..content_validators.doi_validator import DOI_RESOLVER_BASE_URL, DOIValidator


TEST_DOI = "ABCDEFG"


class TestDOIValidator(ValidatorTestCase):
    @patch("requests.get")
    def test_prefix_handling(self, get):
        """ Expect that all known prefixes are removed before trying to resolve the DOI """
        get.configure_mock(return_value=Mock(text='{"responseCode": 1}'))
        validator = DOIValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        publication = spec["publication"][0]
        for prefix in ("doi:", "doi: ", "https://doi.org/"):
            publication["doi"] = f"{prefix}{TEST_DOI}"
            errors = validator.perform_validation(spec)
            self.assertEqual(get.call_args, call(DOI_RESOLVER_BASE_URL + TEST_DOI))
            self.assertEqual(len(errors), 0)
            get.reset_mock()

    @patch("requests.get")
    def test_unresolvable_doi(self, get):
        """ Expect an error for unresolvable DOI """
        get.configure_mock(return_value=Mock(text='{"responseCode": 100}'))
        validator = DOIValidator("test.yaml")
        spec = deepcopy(BASE_SPEC)
        publication = spec["publication"][0]["doi"] = TEST_DOI
        errors = validator.perform_validation(spec)
        self.check_warning_and_error_count(errors, 0, 1)
