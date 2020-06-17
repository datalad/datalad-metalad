
import yaml
import yaml.constructor
from collections import namedtuple
from jsonschema import Draft7Validator
from pathlib import PosixPath
from typing import Any, List, Union
from yaml.error import YAMLError, MarkedYAMLError, Mark
from yaml.scanner import ScannerError

from messages import ValidatorMessage
from content_validators.content_validator import ContentValidator


Typedef = namedtuple("SchemaTypedef", ["id", "schema"])


# Do not convert ISO-date strings in datetime.date objects during yaml.safe_load. To prevent
# this, we overwrite the datetime constructor with the string constructor.
YAML_TIMESTAMP_TAG = "tag:yaml.org,2002:timestamp"
YAML_STRING_TAG = "tag:yaml.org,2002:str"
YAML_CONSTRUCTORS = yaml.constructor.SafeConstructor.yaml_constructors
YAML_CONSTRUCTORS[YAML_TIMESTAMP_TAG] = YAML_CONSTRUCTORS[YAML_STRING_TAG]


class SpecValidator(object):
    @staticmethod
    def _load_yaml_string(yaml_string: str) -> dict:
        return yaml.safe_load(yaml_string)

    @staticmethod
    def _load_spec_object(path_to_schema_spec: PosixPath) -> Any:
        with open(str(path_to_schema_spec), "rt") as spec_token_stream:
            return SpecValidator._load_yaml_string(spec_token_stream.read())

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
                        f"{' ' * len(header)}| please ensure that content with `: ´, i.e. colon followed by space, "
                        f"is enclosed in double quotes, i.e. \".\n")
        return result

    @staticmethod
    def _get_error_description(header: str, error: MarkedYAMLError) -> str:
        result = ""
        for mark in (error.problem_mark, error.context_mark):
            if mark is not None:
                problem = error.problem if hasattr(error, "problem") else "Unknown problem"
                result += SpecValidator._get_mark_description(header, mark, problem)
        return result or f"{header}: {str(error)}"

    def __init__(self, path_to_schema_spec: PosixPath, validators: List[ContentValidator]):
        self.schema = self._load_spec_object(path_to_schema_spec)
        Draft7Validator.check_schema(self.schema)
        self.draft7_validator = Draft7Validator(self.schema)
        self.content_validators = validators
        self.messages = []

    def _validate_spec(self, spec):
        messages = []
        for error in self.draft7_validator.iter_errors(instance=spec):
            messages.append(
                ValidatorMessage(f"Schema error: in {'.'.join(map(str, error.absolute_path))}: {error.message}"))
        return messages

    def validate_spec_object(self, spec) -> bool:
        self.messages = self._validate_spec(spec)
        if not self.messages:
            for content_validator in self.content_validators:
                self.messages += content_validator.perform_validation(spec)
        return not self.messages

    def load_yaml_string(self, yaml_string: str) -> Union[dict, None]:
        self.messages = []
        try:
            return self._load_yaml_string(yaml_string)
        except ScannerError as e:
            self.messages += [ValidatorMessage(self._get_error_description("YAML error", e))]
            return None
        except MarkedYAMLError as e:
            self.messages += [ValidatorMessage(self._get_error_description("YAML error", e))]
            return None
        except YAMLError as e:
            self.messages += [ValidatorMessage(f"YAML error: unknown error{e}")]
            return None

    def validate_spec(self, yaml_string: str) -> bool:
        spec_object = self.load_yaml_string(yaml_string)
        if spec_object is None:
            return False
        return self.validate_spec_object(spec_object)
