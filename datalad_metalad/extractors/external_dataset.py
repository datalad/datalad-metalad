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
from typing import (
    Any,
    Dict,
    IO,
    List,
    Optional,
    Union,
)

from datalad.distribution.dataset import Dataset

from .base import (
    DatasetMetadataExtractor,
    DataOutputCategory,
    ExtractorResult,
)
from .external import ExternalExtractor


lgr = logging.getLogger('datalad.metadata.extractors.metalad_external_dataset')


class ExternalDatasetExtractor(ExternalExtractor, DatasetMetadataExtractor):
    def __init__(self,
                 dataset: Dataset,
                 ref_commit: str,
                 parameter: Dict[str, Any]):

        ExternalExtractor.__init__(self, "dataset", parameter)
        DatasetMetadataExtractor.__init__(self, dataset, ref_commit)

    def _get_args(self) -> List[str]:
        return [self.dataset.path, self.ref_commit]

    def get_required_content(self) -> bool:
        if self.required_content_acquired is False:
            self._execute(["--get-required"] + self._get_args())
            self.required_content_acquired = True
        return self.required_content_acquired

    def get_data_output_category(self) -> DataOutputCategory:
        return ExternalExtractor.get_data_output_category(self)

    def extract(self,
                output_location: Optional[Union[IO, str]] = None
                ) -> ExtractorResult:
        return ExternalExtractor.extract(self, output_location)

    def get_version(self) -> str:
        return ExternalExtractor.get_version(self)
