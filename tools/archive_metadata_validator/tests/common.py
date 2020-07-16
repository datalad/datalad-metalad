
from typing import List
from unittest import TestCase

from messages import ValidatorMessage, WarningMessage, ErrorMessage


PERSON_SPEC = {
    "a@example.com": {
        "given_name": "gn-a",
        "last_name": "ln-a"
    },
    "b@example.com": {
        "given_name": "gn-b",
        "last_name": "ln-b"
    }
}


BASE_SPEC = {
    "person": PERSON_SPEC,
    "study": {
        "principal_investigator": "a@example.com"
    },
    "dataset": {
        "author": "a@example.com",
    },
    "publication": [
        {
            "title": "Publication One",
            "author": [
                "a@example.com",
                "b@example.com"
            ],
            "corresponding_author": "a@example.com",
        }
    ]
}


MINIMAL_SPEC = {
    "person": PERSON_SPEC,
    "study": {
        "principal_investigator": "a@example.com"
    },
    "dataset": {
        "author": "a@example.com",
    }
}


class ValidatorTestCase(TestCase):
    def check_warning_and_error_count(self, errors: List[ValidatorMessage], warning_count: int, error_count: int):
        self.assertEqual(len(errors), warning_count + error_count)
        self.assertEqual(len(list(filter(lambda m: isinstance(m, WarningMessage), errors))), warning_count)
        self.assertEqual(len(list(filter(lambda m: isinstance(m, ErrorMessage), errors))), error_count)
