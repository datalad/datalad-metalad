# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Metadata extractor for files stored in Datalad's own core storage"""
import json
import logging
from typing import (
    Any,
    Dict,
    IO,
    Optional,
    Union,
)

from datalad.distribution.dataset import Dataset

from .base import (
    DataOutputCategory,
    ExtractorResult,
    FileInfo,
    FileMetadataExtractor,
)
from .external import ExternalExtractor


lgr = logging.getLogger('datalad.metadata.extractors.metalad_external_file')


class ExternalFileExtractor(FileMetadataExtractor, ExternalExtractor):
    def __init__(self,
                 dataset: Dataset,
                 ref_commit: str,
                 file_info: FileInfo,
                 parameter: Dict[str, Any]):

        ExternalExtractor.__init__(self, "file", parameter)
        FileMetadataExtractor.__init__(self, dataset, ref_commit, file_info)

    def _get_args(self):
        return [self.dataset.path, self.ref_commit, self.file_info.path,
                self.file_info.intra_dataset_path]

    def is_content_required(self) -> bool:
        if self.content_required is None:
            required = self._execute(self.command_arguments + ["--is-content-required"])
            if required not in ("True", "False"):
                raise ValueError(
                    f"expected 'True' or 'False' from {self.external_command} "
                    f"--is-content-required, got {required}")
            self.content_required = required == "True"
        return self.content_required

    def get_data_output_category(self) -> DataOutputCategory:
        return ExternalExtractor.get_data_output_category(self)

    def extract(self,
                output_location: Optional[Union[IO, str]] = None
                ) -> ExtractorResult:
        return ExternalExtractor.extract(self, output_location)

    def get_version(self) -> str:
        return ExternalExtractor.get_version(self)
