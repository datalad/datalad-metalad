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
import logging
from typing import (
    Any,
    Dict,
    List,
)

from datalad.distribution.dataset import Dataset

from .base import DatasetMetadataExtractor
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
