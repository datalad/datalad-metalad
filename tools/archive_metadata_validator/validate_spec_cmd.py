import sys
from argparse import ArgumentParser
from pathlib import PosixPath
from typing import List

from content_validators.date_validator import DateValidator
from content_validators.doi_validator import DOIValidator
from content_validators.reference_validator import ReferenceValidator
from content_validators.orcidid_validator import ORCIDIDValidator

from validator import SpecValidator


SCRIPT_PATH = PosixPath(sys.argv[0]).parents[0]
SCHEMA_SPEC_PATH = SCRIPT_PATH / "schema" / "archive_metadata.yaml"


PARSER = ArgumentParser()
PARSER.add_argument("spec_files", type=str, nargs="+")
PARSER.add_argument("--skip-content-validation", action="store_true")


def validate_stream(character_stream, skip_content_validation: bool) -> List:
    if skip_content_validation is True:
        validator = SpecValidator(SCHEMA_SPEC_PATH, [])
    else:
        validator = SpecValidator(SCHEMA_SPEC_PATH, [DateValidator(),
                                                     ReferenceValidator(),
                                                     DOIValidator(),
                                                     ORCIDIDValidator()])
    validator.validate_spec(character_stream.read())
    return validator.errors


def validate_file(path, skip_content_validation: bool) -> List:
    with open(path, "rt") as character_stream:
        return validate_stream(character_stream, skip_content_validation)


def main(_):
    arguments = PARSER.parse_args()

    success = True
    for file_name in arguments.spec_files:
        if file_name == '-':
            file_name = "STDIN"
            errors = validate_stream(sys.stdin, arguments.skip_content_validation)
        else:
            errors = validate_file(file_name, arguments.skip_content_validation)
        if errors:
            success = False
            for error in errors:
                for index, line in enumerate(error.splitlines()):
                    if index == 0:
                        sys.stderr.write(f"{file_name}: {line}\n")
                    else:
                        sys.stderr.write(f"{' ' * len(file_name)}  {line}\n")
    return 0 if success is True else 1


if __name__ == "__main__":
    exit(main(sys.argv))
