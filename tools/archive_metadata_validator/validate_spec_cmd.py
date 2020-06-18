import sys
from argparse import ArgumentParser
from functools import reduce
from pathlib import PosixPath
from typing import List

from content_validators.date_validator import DateValidator
from content_validators.doi_validator import DOIValidator
from content_validators.reference_validator import ReferenceValidator
from content_validators.orcidid_validator import ORCIDIDValidator
from content_validators.keyword_validator import KeywordValidator

from validator import SpecValidator


SCRIPT_PATH = PosixPath(sys.argv[0]).parents[0]
SCHEMA_SPEC_PATH = SCRIPT_PATH / "schema" / "archive_metadata.yaml"


PARSER = ArgumentParser()
PARSER.add_argument("spec_files", type=str, nargs="+")
PARSER.add_argument("--skip-content-validation", action="store_true")


def validate_stream(character_stream, file_name, arguments) -> List:
    if arguments.skip_content_validation is True:
        validator = SpecValidator(SCHEMA_SPEC_PATH, [], file_name)
    else:
        validator = SpecValidator(SCHEMA_SPEC_PATH,
                                  [DateValidator(file_name),
                                   ReferenceValidator(file_name),
                                   DOIValidator(file_name),
                                   ORCIDIDValidator(file_name),
                                   KeywordValidator(file_name)],
                                  file_name)
    validator.validate_spec(character_stream.read())
    return validator.messages


def validate_file(path, arguments) -> List:
    with open(path, "rt") as character_stream:
        return validate_stream(character_stream, str(path), arguments)


def main():
    arguments = PARSER.parse_args()

    errors = 0
    for file_name in arguments.spec_files:
        if file_name == '-':
            messages = validate_stream(sys.stdin, "<stdin>", arguments)
        else:
            messages = validate_file(file_name, arguments)

        errors += reduce(lambda summed_score, message: summed_score + message.error_score(), messages, 0)
        tuple(map(lambda message: sys.stderr.write(str(message)), messages))

    return 0 if errors > 0 else 1


if __name__ == "__main__":
    exit(main())
