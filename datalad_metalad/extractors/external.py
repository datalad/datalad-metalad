"""
Common functionality for external extractor shells
"""
import logging
import subprocess
from typing import (
    Any,
    Dict,
    IO,
    List,
    Optional,
    Union,
)
from uuid import UUID

from .base import (
    DataOutputCategory,
    ExtractorResult,
)


lgr = logging.getLogger('datalad.metadata.extractors.external')


class ExternalExtractor:
    def __init__(self,
                 extractor_type: str,
                 parameter: Dict[str, Any]):

        assert extractor_type in ("dataset", "file")
        assert "command" in parameter

        self.parameter = parameter
        self.external_command = self.parameter["command"]
        self.version = self.parameter.get("version", None)
        self.content_required = self.parameter.get("content-required", None)
        provided_extractor_id = self.parameter.get("extractor-id", None)
        provided_output_category_number = self.parameter.get("data-output-category", None)

        self.data_output_category = (
            DataOutputCategory(int(provided_output_category_number))
            if provided_output_category_number is not None
            else None
        )

        for entry in ("command", "extractor-id", "data-output-category", "version"):
            if entry in self.parameter:
                del self.parameter[entry]

        # Remaining elements in self.parameter should be arguments to
        # the external command. Their keys should be integers. They
        # determine the order in which the values are presented to the
        # external extractor.
        self.command_arguments = [
            self.parameter[str(key)]
            for key in sorted(map(int, self.parameter.keys()))
        ]

        self.extractor_id = (
            UUID(provided_extractor_id)
            if provided_extractor_id is not None
            else None)

        self.required_content_acquired = False

    def _execute(self, args: List[str]) -> str:
        return subprocess.run(
            [self.external_command] + args,
            check=True,
            stdout=subprocess.PIPE).stdout.decode().strip()

    def _execute_redirect(self, args: List[str], output: IO):
        subprocess.run(
            [self.external_command] + args,
            check=True,
            stdout=output)

    def get_id(self) -> UUID:
        if self.extractor_id is None:
            self.extractor_id = UUID(self._execute(self.command_arguments + ["--get-uuid"]))
        return self.extractor_id

    def get_version(self) -> str:
        if self.version is None:
            self.version = self._execute(self.command_arguments + ["--get-version"])
        return self.version

    def get_data_output_category(self) -> DataOutputCategory:
        if self.data_output_category is None:
            category = self._execute(self.command_arguments + ["--get-data-output-category"])
            if category == "DIRECTORY":
                self.data_output_category = DataOutputCategory.DIRECTORY
                raise NotImplementedError
            elif category == "FILE":
                self.data_output_category = DataOutputCategory.FILE
            elif category == "IMMEDIATE":
                self.data_output_category = DataOutputCategory.IMMEDIATE
            else:
                raise ValueError(
                    f"expected 'DIRECTORY', or 'FILE', or 'IMMEDIATE' from "
                    f"{self.external_command} --get-output-category, "
                    f"got {category}")
        return self.data_output_category

    def extract(self,
                file_or_name: Optional[Union[IO, str]] = None
                ) -> ExtractorResult:

        args = self.command_arguments + ["--extract"] + self._get_args()
        output_category = self.get_data_output_category()

        if output_category == DataOutputCategory.IMMEDIATE:

            lgr.debug(
                f"calling '{self.external_command} {' '.join(args)}' "
                f"in IMMEDIATE mode")
            immediate_data = self._execute(args)

        elif output_category == DataOutputCategory.FILE:

            lgr.debug(
                f"calling '{self.external_command} {' '.join(args)}' "
                f"in FILE mode")
            self._execute_redirect(args, file_or_name)
            immediate_data = None

        elif output_category == DataOutputCategory.DIRECTORY:

            raise NotImplementedError

        return ExtractorResult(
            extractor_version=self.get_version(),
            extraction_parameter=self.parameter or {},
            extraction_success=True,
            datalad_result_dict={
                "type": "dataset",
                "status": "ok"
            },
            immediate_data=immediate_data)
