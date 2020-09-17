import re
from collections import namedtuple
from typing import List, Optional

import yaml
from error_processor import DuplicatedKey
from messages import ErrorMessage, FileLocation


Location = namedtuple("Location", ("line", "column"))


class YamlMiniParser(object):
    """
    A minimal recursive descent yaml parser, that allows to implement
    fault tolerant parsing and identification of additional errors and
    warnings.
    """
    def __init__(self, file_name: str, loader: type(yaml.Loader), key_list: List[str]):
        self.file_name = file_name
        self.loader = loader
        self.key_list = key_list
        self.current_token = None
        self.errors = []
        self.object_locations = {}

    def _get_current_token_value(self):
        if hasattr(self.current_token, "value"):
            return self.current_token.value
        else:
            return type(self.current_token).__name__

    def add_error(self, error):
        self.errors.append(error)

    def is_key(self, value: str) -> bool:
        match = re.match("<([a-z]*)( .*)?>", value)
        if match:
            return f"<{match.group(1)}>" in self.key_list
        match = re.match("([a-z]*)(_.*)?", value)
        if match:
            return f"{match.group(1)}" in self.key_list
        return value in self.key_list

    def _assert_token_type(self, token_type: type(yaml.Token)):
        if not isinstance(self.current_token, token_type):
            self.add_error(
                ErrorMessage(
                    f"expected {token_type.__name__}, but got: {type(self.current_token).__name__}",
                    FileLocation(
                        self.file_name,
                        self.current_token.start_mark.line + 1,
                        self.current_token.start_mark.column + 1)))

            raise ValueError(
                f"expected {token_type.__name__}, but got: {type(self.current_token).__name__}")

    def _report_expected_error(self, expected: str, path):
        self.add_error(
            ErrorMessage(
                f"expected {expected} token, but got {self._get_current_token_value()}",
                FileLocation(
                    self.file_name,
                    self.current_token.start_mark.line + 1,
                    self.current_token.start_mark.column + 1)))
        raise ValueError(
            f"expected {expected} token, but got "
            f"{self._get_current_token_value()} at path {'.'.join(path)}")

    def get_token(self, expect_token: Optional[type(yaml.Token)] = None) -> yaml.Token:
        self.current_token = self.loader.get_token()
        if expect_token:
            self._assert_token_type(expect_token)
        return self.current_token

    def consume_token(self, expect_token: type(yaml.Token)):
        self._assert_token_type(expect_token)
        self.get_token()

    def matches(self, expected_token: type(yaml.Token)):
        return isinstance(self.current_token, expected_token)

    def parse_mapping(self, path: List[str]) -> dict:
        result = {}
        while isinstance(self.current_token, yaml.KeyToken):
            self.get_token(yaml.ScalarToken)
            key_token = self.current_token
            self.get_token(yaml.ValueToken)
            self.get_token()
            key = key_token.value
            if key in result:
                self.add_error(DuplicatedKey(key, path + [key]))
            new_path = path + [key]
            result[key] = self.parse_value(new_path)
            self.object_locations[".".join(new_path)] = Location(
                key_token.start_mark.line, key_token.start_mark.column)

        self.consume_token(yaml.BlockEndToken)
        return result

    def parse_value(self, path: List[str]):
        if self.matches(yaml.ScalarToken):
            result = self.current_token.value
            self.object_locations[".".join(path)] = Location(
                self.current_token.start_mark.line, self.current_token.start_mark.column)
            self.get_token()
            return result
        elif self.matches(yaml.BlockMappingStartToken):
            self.get_token()
            return self.parse_mapping(path)
        elif self.matches(yaml.BlockSequenceStartToken):
            self.get_token()
            return self.parse_sequence(path)
        else:
            self._report_expected_error("scalar, mapping-start, or list-start", path)

    def parse_sequence(self, path: List[str]):
        result = []
        index = 0
        while self.matches(yaml.BlockEntryToken):
            self.get_token()
            new_path = path + [f"[{index}]"]
            result.append(self.parse_value(new_path))
            index += 1
        self.consume_token(yaml.BlockEndToken)
        return result

    def parse_document(self, path: List[str]):
        while not self.matches(yaml.StreamEndToken):
            if self.matches(yaml.BlockMappingStartToken):
                self.get_token()
                return self.parse_mapping(path)
            elif self.matches(yaml.BlockSequenceStartToken):
                self.get_token()
                return self.parse_sequence(path)
            else:
                self._report_expected_error("mapping-start, or list-start", path)

    def parse_stream(self):
        self.get_token(yaml.StreamStartToken)
        self.get_token()
        self.object_locations = {}
        try:
            result = self.parse_document([])
            self.consume_token(yaml.StreamEndToken)
            return result
        except ValueError:
            return None
