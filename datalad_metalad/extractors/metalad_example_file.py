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
import time
from uuid import UUID

from .. import get_file_id
from .base import DataOutputCategory, ExtractorResult, FileMetadataExtractor


lgr = logging.getLogger('datalad.metadata.extractors.metalad_example_file')


class MetaladExampleFileExtractor(FileMetadataExtractor):

    def get_data_output_category(self) -> DataOutputCategory:
        return DataOutputCategory.IMMEDIATE

    def is_content_required(self) -> bool:
        return True

    def get_id(self) -> UUID:
        return UUID("89fae179-eceb-4af2-8088-dfebdae6e2c0")

    def get_version(self) -> str:
        return "0.0.1"

    def extract(self, _=None) -> ExtractorResult:
        return ExtractorResult(
            extractor_version=self.get_version(),
            extraction_parameter=self.parameter or {},
            extraction_success=True,
            datalad_result_dict={
                "type": "file",
                "status": "ok"
            },
            immediate_data={
                "@id": get_file_id(dict(
                    path=self.file_info.path,
                    type=self.file_info.type)),
                "type": self.file_info.type,
                "path": self.file_info.intra_dataset_path,
                "content_byte_size": self.file_info.byte_size,
                "comment": f"example file extractor executed at {time.time()}"
            })
