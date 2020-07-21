
import yaml
import yaml.constructor

from jsonschema import Draft7Validator, ValidationError
from pathlib import PosixPath
from typing import Any, List, Optional, Tuple, Union
from yaml.error import YAMLError, MarkedYAMLError

import error_processor
from messages import ErrorMessage, FileLocation, ObjectLocation, WarningMessage
from content_validators.content_validator import ContentValidator, ContentValidatorInfo
from indent_sanitizer import YamlIndentationSanitizer
from yaml_mini_parser import YamlMiniParser


# Do not convert ISO-date strings in datetime.date objects during yaml.safe_load. To prevent
# this, we overwrite the datetime constructor with the string constructor. We cannot simply
# use BaseLoader, because we still rely on integer conversions.
YAML_TIMESTAMP_TAG = "tag:yaml.org,2002:timestamp"
YAML_STRING_TAG = "tag:yaml.org,2002:str"
YAML_CONSTRUCTORS = yaml.constructor.SafeConstructor.yaml_constructors
YAML_CONSTRUCTORS[YAML_TIMESTAMP_TAG] = YAML_CONSTRUCTORS[YAML_STRING_TAG]


OBJECT_HIERARCHY = {
    "study": {
        "name": True,
        "principal_investigator": True,
        "keyword": [
            True
        ],
        "funding": False,
        "purpose": False,
        "start_date": False,
        "end_date": False,
        "contributor": False
    },
    "dataset": {
        "name": True,
        "location": True,
        "author": [
            {
                "<email>": True
            }
        ],
        "keyword": {
            "<word>": False
        },
        "description": False,
        "funding": False,
        "standard": False
    },
    "person": {
        "<email>": {
            "given_name": True,
            "last_name": True,
            "orcid-id": False,
            "title": False,
            "affiliation": False,
            "contact_information": False
        }
    },
    "publication": [
        {
            "title": False,
            "author": False,
            "year": False,
            "corresponding_author": False,
            "doi": False,
            "publication": False,
            "volume": False,
            "issue": False,
            "pages": False,
            "publisher": False
        }
    ],
    "additional_information": False
}


def possible_paths(item: Union[List, dict, Any], prefix: Optional[str] = "") -> List[str]:
    result = []
    if isinstance(item, dict):
        for key, value in item.items():
            result += possible_paths(value, ("" if prefix is "" else prefix + ".") + key)
    elif isinstance(item, list):
        for index, element in enumerate(item):
            result += possible_paths(element, prefix + "[" + str(index) + "]")
    else:
        result += [("" if prefix is "" else prefix + ".") + f"<content value='{item}'>"]
    return result


def all_keys(info: dict) -> set:
    result = set()
    for key, value in info.items():
        result.add(key)
        if isinstance(value, dict):
            result |= all_keys(value)
    return result


class SpecValidator(object):
    @staticmethod
    def _load_yaml_string(yaml_string: str, loader: Optional[type(yaml.Loader)] = yaml.SafeLoader) -> dict:
        return yaml.load(yaml_string, Loader=loader)

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

    def _create_schema_error(self, error: ValidationError):
        # TODO: invoke error interpretation in error_processor.
        self.schema_errors.append(error_processor.classify_validation_error(error))

    def __init__(self, path_to_schema_spec: PosixPath, validator_infos: List[ContentValidatorInfo], file_name: str):
        self.schema = self._load_spec_object(path_to_schema_spec)
        Draft7Validator.check_schema(self.schema)
        self.draft7_validator = Draft7Validator(self.schema)
        self.content_validator_infos = validator_infos
        self.file_name = file_name
        self.messages = []
        self.yaml_errors = []
        self.source_position = {}
        self.schema_errors = []

    def _create_schema_error_messages(self, object_locations) -> List[ErrorMessage]:
        result = []
        for error in self.schema_errors:
            dotted_path_name = ContentValidator.path_to_dotted_name(error.path)
            result.append(
                ErrorMessage(str(error), ObjectLocation(self.file_name, dotted_path_name, object_locations)))
        return result

    def load_yaml_string(self, yaml_string: str) -> Union[dict, None]:
        try:
            spec_object = self._load_yaml_string(yaml_string)
            if spec_object is None:
                self.messages += [
                    ErrorMessage(
                        "empty document", FileLocation(self.file_name, 0, 0))]
            return spec_object
        except YAMLError as e:
            self.messages += [self._create_yaml_error(e)]
            return None

    def sanitize_yaml(self, yaml_string: str, key_list: List) -> Tuple[str, Optional[dict]]:
        sanitizer = YamlIndentationSanitizer(yaml_string, key_list)
        if sanitizer.sanitize() is True:
            for line, correction in sorted(list(sanitizer.corrections), key=lambda e: e[0]):
                self.messages += [
                    WarningMessage(
                        correction, FileLocation(self.file_name, line + 1, 0))]
            if sanitizer.document != sanitizer.original_document:
                self.messages += [
                    WarningMessage(
                        f"using corrected document as input:\n{'------8<' * 10}------\n"
                        f"{sanitizer.document}\n{'------8<' * 10}------\n",
                        FileLocation(self.file_name, 0, 0))]
            parser = YamlMiniParser(yaml.BaseLoader(sanitizer.document), key_list)
            parser.parse_stream()
            return sanitizer.document, parser.object_locations
        return yaml_string, None

    def _validate_spec(self, spec, object_locations) -> List[ErrorMessage]:
        for error in self.draft7_validator.iter_errors(instance=spec):
            self._create_schema_error(error)
        return self._create_schema_error_messages(object_locations)

    def validate_spec_object(self, spec, object_locations: dict) -> bool:
        self.messages += self._validate_spec(spec, object_locations)
        for content_validator_info in self.content_validator_infos:
            self.messages += content_validator_info.create(self.file_name, spec, object_locations).perform_validation()
        self.messages.sort(key=lambda m: m.location.line)
        return not self.messages

    def validate_spec(self, yaml_string: str) -> bool:
        self.messages = []
        corrected_yaml_string, object_locations = self.sanitize_yaml(yaml_string, list(all_keys(OBJECT_HIERARCHY)))
        spec_object = self.load_yaml_string(corrected_yaml_string)
        if spec_object is None:
            return False
        return self.validate_spec_object(spec_object, object_locations)
