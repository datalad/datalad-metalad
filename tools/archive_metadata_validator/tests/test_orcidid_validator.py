from copy import deepcopy

from messages import WarningMessage, ErrorMessage
from .common import BASE_SPEC, ValidatorTestCase
from ..content_validators.orcidid_validator import ORCIDIDValidator


ORCID_ID_PREFIX = "https://orcid.org/"

VALID_ORCID_ID = "0000-0003-1756-9271"
INVALID_ORCID_ID = "0000-0003-1756-9273"
MALFORMED_ORCID_ID = "0000+0003-1756-9273"


class TestORCIDIDValidator(ValidatorTestCase):
    def test_valid_orcid_id(self):
        """ Expect no warning or error on a valid ORCID ID with the ORCID ID prefix """
        spec = deepcopy(BASE_SPEC)
        spec["person"]["a@example.com"]["orcid-id"] = ORCID_ID_PREFIX + VALID_ORCID_ID
        validator = ORCIDIDValidator("test.yaml", spec)
        errors = validator.perform_validation()
        self.assertEqual(len(errors), 0)

    def test_prefixless_valid_orcid_id(self):
        """ Expect a warning on a valid ORCID ID without the ORCID ID prefix """
        spec = deepcopy(BASE_SPEC)
        spec["person"]["a@example.com"]["orcid-id"] = VALID_ORCID_ID
        validator = ORCIDIDValidator("test.yaml", spec)
        errors = validator.perform_validation()
        self.check_warning_and_error_count(errors, 1, 0)

    def test_orcid_id_checksum_error(self):
        """ Expect a warning and an error on a ORCID ID with checksum error and without the ORCID ID prefix """
        spec = deepcopy(BASE_SPEC)
        spec["person"]["a@example.com"]["orcid-id"] = INVALID_ORCID_ID
        validator = ORCIDIDValidator("test.yaml", spec)
        errors = validator.perform_validation()
        self.check_warning_and_error_count(errors, 1, 1)

    def test_orcid_id_malformed(self):
        """ Expect an error malformed ORCID ID without the ORCID ID prefix """
        spec = deepcopy(BASE_SPEC)
        spec["person"]["a@example.com"]["orcid-id"] = ORCID_ID_PREFIX + MALFORMED_ORCID_ID
        validator = ORCIDIDValidator("test.yaml", spec)
        errors = validator.perform_validation()
        self.check_warning_and_error_count(errors, 0, 1)

    def test_duplicated_orcid_id(self):
        """ Expect a warning or duplicated ORCID ID """
        spec = deepcopy(BASE_SPEC)
        spec["person"]["a@example.com"]["orcid-id"] = ORCID_ID_PREFIX + VALID_ORCID_ID
        spec["person"]["b@example.com"]["orcid-id"] = ORCID_ID_PREFIX + VALID_ORCID_ID
        validator = ORCIDIDValidator("test.yaml", spec)
        errors = validator.perform_validation()
        self.check_warning_and_error_count(errors, 1, 0)

