
import yaml
import yaml.constructor
from collections import namedtuple
from jsonschema import Draft7Validator, ValidationError
from pathlib import PosixPath
from typing import Any, List, Union
from yaml.error import YAMLError, MarkedYAMLError

from messages import ErrorMessage, FileLocation
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

    def _get_error_location(self, error: YAMLError) -> FileLocation:
        if isinstance(error, MarkedYAMLError):
            mark = error.problem_mark
            if mark is not None:
                return FileLocation(self.file_name, mark.line + 1, mark.column + 1)
        return FileLocation(self.file_name, 0, 0)

    def _create_yaml_error(self, error: YAMLError) -> ErrorMessage:
        problem = error.problem if hasattr(error, "problem") else "unknown error"
        location = self._get_error_location(error)
        description = f"YAML parsing error: {problem}\n"
        if isinstance(error, MarkedYAMLError):
            mark = error.problem_mark
            if mark and mark.buffer and location.line != 0 and location.column != 0:
                source_lines = mark.buffer.splitlines()
                description += (
                    f"| {source_lines[mark.line]}\n"
                    f"| {'-' * mark.column}^\n")
                if problem.startswith("mapping values are not allowed here"):
                    if ":" in source_lines[mark.line]:
                        description += (
                            f"| please ensure that content with `: ´, i.e. colon followed by space, "
                            f"is enclosed in double quotes, i.e. ` \"´.\n")
        return ErrorMessage(description, location)

    def _create_schema_error(self, error: ValidationError) -> ErrorMessage:
        return ErrorMessage(f"schema violation: {error.message}", FileLocation(self.file_name, 0, 0))

    def __init__(self, path_to_schema_spec: PosixPath, validators: List[ContentValidator], file_name: str):
        self.schema = self._load_spec_object(path_to_schema_spec)
        Draft7Validator.check_schema(self.schema)
        self.draft7_validator = Draft7Validator(self.schema)
        self.content_validators = validators
        self.file_name = file_name
        self.messages = []

    def _validate_spec(self, spec):
        messages = []
        for error in self.draft7_validator.iter_errors(instance=spec):
            messages += [self._create_schema_error(error)]
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
        except YAMLError as e:
            self.messages += [self._create_yaml_error(e)]
            return None

    def validate_spec(self, yaml_string: str) -> bool:
        spec_object = self.load_yaml_string(yaml_string)
        if spec_object is None:
            return False
        return self.validate_spec_object(spec_object)
