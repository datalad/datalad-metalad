
import yaml
from collections import namedtuple
from pathlib import PosixPath
from typing import Any, List
from yaml.error import YAMLError, MarkedYAMLError

from content_validators.content_validator import ContentValidator
from jsonschema import validate, ValidationError


Typedef = namedtuple("SchemaTypedef", ["id", "schema"])


class SpecValidator(object):
    @staticmethod
    def _create_schema_from_spec(definitions):
        schema_creator = Rx.Factory({"register_core_types": True})
        tuple(
            map(
                lambda typedef: schema_creator.learn_type(typedef.id, typedef.schema),
                (Typedef(assoc["id"], assoc["schema"]) for assoc in definitions.get("typedefs", []))
            )
        )
        return schema_creator.make_schema(definitions["schema"])

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
        self.content_validators = validators
        self.errors = []

    def _validate_spec(self, spec):
        try:
            validate(instance=spec, schema=self.schema)
            return []
        except ValidationError as error:
            return [f"Schema error: in {'.'.join(error.absolute_path)}: {error.message}"]

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
