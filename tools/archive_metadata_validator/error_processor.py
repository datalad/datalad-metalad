import re
from typing import List, Optional, Union

from jsonschema import ValidationError

from content_validators.content_validator import ContentValidator


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


def path_to_str(path: List[Union[int, str]]) -> str:
    return ContentValidator.unescape_name(ContentValidator.path_to_dotted_name(path))


class Unknown(object):
    def __init__(self, message: str, path: List[Union[int, str]]):
        self.message = message
        self.path = path

    def __str__(self):
        return f"error in element '{path_to_str(self.path)}': {self.message}"


class UnexpectedKey(object):
    def __init__(self, key: str, path: List[Union[int, str]], additional_message: Optional[str] = None):
        self.key = key
        self.path = path
        self.additional_message = additional_message

    def __str__(self):
        return f"unexpected key '{path_to_str(self.path)}'"


class UnexpectedIntegerKey(object):
    def __init__(self, key_representation: str, path: List[Union[int, str]], additional_message: Optional[str] = None):
        self.key = int(key_representation)
        self.path = path
        self.additional_message = additional_message

    def __str__(self):
        return f"unexpected key '{path_to_str(self.path)}'"


class MissingKey(object):
    def __init__(self, key: str, path: List[Union[int, str]], additional_message: Optional[str] = None):
        self.key = key
        self.path = path
        self.additional_message = additional_message

    def __str__(self):
        return f"missing key '{path_to_str(self.path)}'"


class InvalidType(object):
    def __init__(self, value: str, path: List[Union[int, str]], expected_type: str):
        self.value = value
        self.path = path
        self.expected_type = expected_type

    def __str__(self):
        return f"invalid type at '{path_to_str(self.path)}', '{self.value}' is not of type {self.expected_type}"


class DuplicatedKey(object):
    def __init__(self, key: str, path: List[Union[int, str]]):
        self.key = key
        self.path = path

    def __str__(self):
        if self.key in POSSIBLE_ENUMERATED_KEYS:
            return f"duplicated key '{path_to_str(self.path)}', did you forget a '-'?"
        else:
            return f"duplicated key '{path_to_str(self.path)}'"


class InvalidYear(object):
    def __init__(self, invalid_year: str, path: List[Union[int, str]]):
        self.invalid_year = invalid_year
        self.path = path

    def __str__(self):
        return f"invalid year ({self.invalid_year}) '{path_to_str(self.path)}'"


class ValueWarning(object):
    def __init__(self, invalid_value: str, path: List[Union[int, str]], warning_message: str):
        self.invalid_value = invalid_value
        self.path = path
        self.warning_message = warning_message

    def __str__(self):
        return f"invalid year ({self.invalid_value}) '{path_to_str(self.path)}'"


def classify_validation_error(error: ValidationError):
    if error.validator == ADDITIONAL_PROPERTIES_VALIDATOR and error.validator_value is False:
        match = re.match(r"Additional properties are not allowed \('([a-z_@0-9\-]*)'", error.message)
        if match:
            return UnexpectedKey(match.group(1), list(map(str, error.path)) + [match.group(1)])
        else:
            match = re.match(r"Additional properties are not allowed \(([0-9]+)", error.message)
            if match:
                return UnexpectedIntegerKey(match.group(1), list(map(str, error.path)) + [match.group(1)])
            return UnexpectedKey(
                "<unknown>",
                list(error.path),
                f"One of the following keys is not allowed: {','.join(map(str, error.instance.keys()))}")

    elif error.validator == REQUIRED_VALIDATOR:
        match = re.match(r"'([a-z_@\-0-9]*)' is a required property", error.message)
        if match:
            return MissingKey(match.group(1), list(map(str, error.path)) + [match.group(1)])
        else:
            return MissingKey(
                "<unknown>",
                list(error.path),
                f"one of: {','.join(error.validator_value)} is required")

    elif error.validator == TYPE_VALIDATOR:
        return InvalidType(error.instance, list(map(str, error.path)), error.validator_value)

    raise Exception("implement yaml error classifier")
