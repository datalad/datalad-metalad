# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""
Shell for external dataset extractors
"""
import json
import logging
import subprocess
from typing import Any, Dict, IO, List, Optional, Union
from uuid import UUID

from datalad.distribution.dataset import Dataset

from .base import DataOutputCategory, ExtractorResult, DatasetMetadataExtractor


lgr = logging.getLogger('datalad.metadata.extractors.metalad_core_dataset')


class ExternalDatasetExtractor(DatasetMetadataExtractor):
    def __init__(self,
                 dataset: Dataset,
                 ref_commit: str,
                 parameter: Optional[Dict[str, Any]] = None):

        parameter = parameter or {}
        DatasetMetadataExtractor.__init__(self, dataset, ref_commit, parameter)
        self.required_content_acquired = False

        self.external_command = self.parameter["command"]
        self.extractor_id = self.parameter.get("extractor-id", None)
        self.data_output_category = self.parameter.get("data-output-category", None)
        self.version = self.parameter.get("version", None)

        for entry in ("command", "extractor-id", "data-output-category", "version"):
            if entry in self.parameter:
                del self.parameter[entry]

        self.extractor_id = (
            UUID(self.extractor_id)
            if self.extractor_id is not None
            else None)

    def _get_args(self) -> List[str]:
        parameter_arg = json.dumps(self.parameter)
        return [self.dataset.path, self.ref_commit, parameter_arg]

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
            self.extractor_id = UUID(self._execute(["--get-uuid"]))
        return self.extractor_id

    def get_version(self) -> str:
        if self.version is None:
            self.version = self._execute(["--get-version"])
        return self.version

    def get_data_output_category(self) -> DataOutputCategory:
        if self.data_output_category is None:
            category = self._execute(["--get-data-output-category"])
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

    def get_required_content(self) -> bool:
        if self.required_content_acquired is False:
            self._execute(["--get-required"] + self._get_args())
            self.required_content_acquired = True
        return self.required_content_acquired

    def extract(self,
                file_or_name: Optional[Union[IO, str]] = None
                ) -> ExtractorResult:

        args = ["--extract"] + self._get_args()
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
