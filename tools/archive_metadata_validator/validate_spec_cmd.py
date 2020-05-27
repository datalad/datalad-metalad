import sys
from argparse import ArgumentParser
from pathlib import PosixPath
from typing import List

import yaml
from content_validators.date_validator import DateValidator
from content_validators.reference_validator import ReferenceValidator
from validator import SpecValidator


SCRIPT_PATH = PosixPath(sys.argv[0]).parents[0]
SCHEMA_SPEC_PATH = SCRIPT_PATH / "schema" / "archive_metadata.yaml"


PARSER = ArgumentParser()
PARSER.add_argument("spec_files", type=str, nargs="+")


def validate_stream(character_stream) -> List:
    validator = SpecValidator(SCHEMA_SPEC_PATH, [DateValidator(), ReferenceValidator()])
    validator.validate_spec(character_stream)
    return validator.errors


def validate_file(path) -> List:
    with open(path, "rt") as character_stream:
        return validate_stream(character_stream)


def main(argument_list):
    arguments = PARSER.parse_args()

    success = True
    for file_name in arguments.spec_files:
        if file_name == '-':
            file_name = "STDIN"
            errors = validate_stream(sys.stdin)
        else:
            errors = validate_file(file_name)
        if errors:
            success = False
            for error in errors:
                print(f"{file_name}: {error}")
    return 0 if success is True else 1


if __name__ == "__main__":
    exit(main(sys.argv))
