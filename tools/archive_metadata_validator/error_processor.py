import re
from typing import List, Optional

from jsonschema import ValidationError


ADDITIONAL_PROPERTIES_VALIDATOR = "additionalProperties"
REQUIRED_VALIDATOR = "required"
TYPE_VALIDATOR = "type"


# Keys that may appear as first element of a listed objects.
POSSIBLE_ENUMERATED_KEYS = {
    "title",
    "author",
    "year",
    "corresponding_author",
    "doi",
    "publication",
    "volume",
    "issue",
    "pages",
    "publisher"
}


class Unknown(object):
    def __init__(self, message: str, path: List[str]):
        self.message = message
        self.path = path

    def __str__(self):
        return f"error in element '{'.'.join(self. path)}': {self.message}"


class UnexpectedKey(object):
    def __init__(self, key: str, path: List[str], additional_message: Optional[str] = None):
        self.key = key
        self.path = path
        self.additional_message = additional_message

    def __str__(self):
        return f"unexpected key '{'.'.join(self. path + [self.key])}'"


class MissingKey(object):
    def __init__(self, key: str, path: List[str], additional_message: Optional[str] = None):
        self.key = key
        self.path = path
        self.additional_message = additional_message

    def __str__(self):
        return f"missing key '{'.'.join(self. path + [self.key])}'"


class InvalidType(object):
    def __init__(self, value: str, path: List[str], expected_type: str):
        self.value = value
        self.path = path
        self.expected_type = expected_type

    def __str__(self):
        return f"invalid type at '{'.'.join(self. path)}', '{self.value}' is not of type {self.expected_type}"


class DuplicatedKey(object):
    def __init__(self, key: str, path: List[str]):
        self.key = key
        self.path = path

    def __str__(self):
        if self.key in POSSIBLE_ENUMERATED_KEYS:
            return f"duplicated key '{'.'.join(self.path + [self.key])}', did you forget a '-'?"
        else:
            return f"duplicated key '{'.'.join(self. path + [self.key])}'"


class InvalidYear(object):
    def __init__(self, invalid_year: str, path: List[str]):
        self.invalid_year = invalid_year
        self.path = path

    def __str__(self):
        return f"invalid year ({self.invalid_year}) '{'.'.join(self. path)}'"


class ValueWarning(object):
    def __init__(self, invalid_value: str, path: List[str], warning_message: str):
        self.invalid_value = invalid_value
        self.path = path
        self.warning_message = warning_message

    def __str__(self):
        return f"invalid year ({self.invalid_value}) '{'.'.join(self. path)}'"


def classify_validation_error(error: ValidationError):
    if error.validator == ADDITIONAL_PROPERTIES_VALIDATOR and error.validator_value is False:
        match = re.match(r"Additional properties are not allowed \('([a-z_@\-]*)'", error.message)
        if match:
            return UnexpectedKey(match.group(1), list(map(str, error.path)))
        else:
            match = re.match(r"Additional properties are not allowed \(([0-9]+)", error.message)
            if match:
                return UnexpectedKey(match.group(1), list(map(str, error.path)))
            return UnexpectedKey(
                "<unknown>",
                list(error.path),
                f"One of the following keys is not allowed: {','.join(map(str, error.instance.keys()))}")

    elif error.validator == REQUIRED_VALIDATOR:
        match = re.match(r"'([a-z_@\-]*)' is a required property", error.message)
        if match:
            return MissingKey(match.group(1), list(map(str, error.path)))
        else:
            return MissingKey(
                "<unknown>",
                list(error.path),
                f"one of: {','.join(error.validator_value)} is required")

    elif error.validator == TYPE_VALIDATOR:
        return InvalidType(error.instance, list(map(str, error.path)), error.validator_value)

    raise Exception("implement yaml error classifier")
