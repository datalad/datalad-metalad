
import yaml
from collections import namedtuple
from pathlib import PosixPath
from typing import Any, List
from yaml.error import YAMLError, MarkedYAMLError

from content_validators.content_validator import ContentValidator
from jsonschema import Draft7Validator


Typedef = namedtuple("SchemaTypedef", ["id", "schema"])


class SpecValidator(object):
    @staticmethod
    def _load_spec_object(path_to_schema_spec) -> Any:
        with open(str(path_to_schema_spec), "rt") as spec_token_stream:
            spec_object = yaml.safe_load(spec_token_stream)
            return spec_object

    @staticmethod
    def _create_error(header: str, errors: tuple) -> List:
        return [f"{header}: {error}" for error in errors]

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

    def validate_spec(self, yaml_token_stream) -> bool:
        try:
            spec_object = yaml.safe_load(yaml_token_stream)
        except MarkedYAMLError as e:
            self.errors = [f"YAML error:{e.problem_mark}: {e.problem} {e.context} {e.context_mark}"]
            return False
        except YAMLError as e:
            self.errors = [f"YAML error: {e}"]
            return False
        return self.validate_spec_object(spec_object)
