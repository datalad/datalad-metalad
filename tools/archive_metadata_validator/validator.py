
import yaml
from collections import namedtuple
from pathlib import PosixPath
from typing import Any, List, Union
from yaml.error import Mark
from yaml.scanner import ScannerError

from content_validators.content_validator import ContentValidator
from jsonschema import Draft7Validator


Typedef = namedtuple("SchemaTypedef", ["id", "schema"])


class SpecValidator(object):
    @staticmethod
    def _load_spec_object(path_to_schema_spec) -> Any:
        with open(str(path_to_schema_spec), "rt") as spec_token_stream:
            spec_object = yaml.safe_load(spec_token_stream.read())
            return spec_object


    @staticmethod
    def _get_mark_description(header: str, mark: Mark, problem: str) -> str:
        if mark is None:
            return f"{header}: line: ???: {problem}\n"

        source_lines = mark.buffer.splitlines()
        result = (
            f"{header}: line: {mark.line + 1}: column: {mark.column + 1}: {problem}\n"
            f"{' ' * len(header)}| {source_lines[mark.line]}\n"
            f"{' ' * len(header)}| {'-' * mark.column}^\n")

        if problem.startswith("mapping values are not allowed here"):
            if ":" in source_lines[mark.line]:
                result += (
                        f"{' ' * len(header)}| please ensure that content with `:´ "
                        f"is enclosed in double quotes, i.e. \".\n")
        return result

    @staticmethod
    def _get_error_description(header: str, error: ScannerError) -> str:
        result = ""
        for mark in (error.problem_mark, error.context_mark):
            if mark is not None:
                result += SpecValidator._get_mark_description(
                    header, error.problem_mark, error.problem or "Unknown problem")
        return result or f"{header}: {str(error)}"

    def __init__(self, path_to_schema_spec: PosixPath, validators: List[ContentValidator]):
        self.schema = self._load_spec_object(path_to_schema_spec)
        Draft7Validator.check_schema(self.schema)
        self.draft7_validator = Draft7Validator(self.schema)
        self.content_validators = validators
        self.errors = []

    def _validate_spec(self, spec):
        errors = []
        for error in self.draft7_validator.iter_errors(instance=spec):
            errors.append(f"Schema error: in {'.'.join(map(str, error.absolute_path))}: {error.message}")
        return errors

    def validate_spec_object(self, spec) -> bool:
        self.errors = self._validate_spec(spec)
        if not self.errors:
            for content_validator in self.content_validators:
                self.errors += content_validator.perform_validation(spec)
        return not self.errors

    def validate_spec(self, yaml_token_stream: str) -> bool:
        try:
            spec_object = yaml.safe_load(yaml_token_stream)
        except ScannerError as e:
            self.errors = [self._get_error_description("YAML error", e)]
            return False
        return self.validate_spec_object(spec_object)
