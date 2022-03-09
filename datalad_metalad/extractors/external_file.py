# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""MetadataRecord extractor for files stored in Datalad's own core storage"""
import logging
from typing import (
    Any,
    Dict,
)

from datalad.distribution.dataset import Dataset

from .base import (
    FileInfo,
    FileMetadataExtractor,
)
from .external import ExternalExtractor


lgr = logging.getLogger('datalad.metadata.extractors.metalad_external_file')


class ExternalFileExtractor(ExternalExtractor, FileMetadataExtractor):
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
