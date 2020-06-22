
from .common import ValidatorTestCase
from ..content_validators.extag_validator import ExTagValidator, MAX_EXTAG_REPORTS


class TestExTagValidator(ValidatorTestCase):
    def _check_line_numbers(self, message, line_number):
        self.assertEqual(len(message.text.splitlines()), line_number)

    def test_no_extag(self):
        """ Expect no warning or error """
        spec = {f"{i}": f"{i}" for i in range(2 * MAX_EXTAG_REPORTS)}
        validator = ExTagValidator("test.yaml")
        errors = validator.perform_validation(spec)
        self.assertEqual(len(errors), 0)

    def test_left_extag(self):
        """ Expect only MAX_EXTAG_REPORTS - 1 + 1 lines if MAX_EXTAG_REPORTS - 1 ex-tags are in content """
        spec = {f"{i}": f"<ex>{i}" for i in range(MAX_EXTAG_REPORTS - 1)}
        validator = ExTagValidator("test.yaml")
        errors = validator.perform_validation(spec)
        self.check_warning_and_error_count(errors, 1, 0)
        self._check_line_numbers(errors[0], MAX_EXTAG_REPORTS)

    def test_right_extag(self):
        """ Expect only MAX_EXTAG_REPORTS + 1 lines if exact MAX_EXTAG_REPORTS ex-tags are in content """
        spec = {f"{i}": f"{i}</ex>" for i in range(MAX_EXTAG_REPORTS)}
        validator = ExTagValidator("test.yaml")
        errors = validator.perform_validation(spec)
        self.check_warning_and_error_count(errors, 1, 0)
        self._check_line_numbers(errors[0], MAX_EXTAG_REPORTS + 1)

    def test_extag(self):
        """ Expect only MAX_EXTAG_REPORTS + 2 lines if more than in content """
        spec = {f"{i}": f"<ex>{i}</ex>" for i in range(3 * MAX_EXTAG_REPORTS)}
        validator = ExTagValidator("test.yaml")
        errors = validator.perform_validation(spec)
        self.check_warning_and_error_count(errors, 1, 0)
        self._check_line_numbers(errors[0], MAX_EXTAG_REPORTS + 2)

    def test_listed_extag(self):
        """ Expect only MAX_EXTAG_REPORTS + 2 lines if more than in content """
        spec = [f"<ex>{i}</ex>" for i in range(3 * MAX_EXTAG_REPORTS)]
        validator = ExTagValidator("test.yaml")
        errors = validator.perform_validation({"content": spec})
        self.check_warning_and_error_count(errors, 1, 0)
        self._check_line_numbers(errors[0], MAX_EXTAG_REPORTS + 2)
