# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""MetadataRecord extractor for custom (JSON-LD) metadata contained in a dataset

One or more source files with metadata can be specified via the
'datalad.metadata.custom-dataset-source' configuration variable.
The content of these files must be a JSON object, and a metadata
dictionary is built by updating it with the content of the JSON
objects in the order in which they are given.

By default a single file is read: '.metadata/dataset.json'
"""

from datalad_metalad.extractors.base import (
    FileMetadataExtractor,
    DataOutputCategory,
    ExtractorResult,
    FileInfo,
)

from pathlib import (
    Path,
    PurePosixPath,
)
from uuid import UUID
import logging

lgr = logging.getLogger("datalad.metadata.extractors.custom_file")
from datalad.interface.results import get_status_dict
from datalad.log import log_progress
from datalad.support.json_py import load as jsonload
from datalad.support.exceptions import CapturedException
from datalad.utils import (
    ensure_list,
)
from typing import Generator


# what does a user have to do before running custom file extractor:
# - use the default config to place metadata files inside `.metadata/content
# - specify their own config using `dataset.config.set("datalad.metadata.custom-content-source",arg)``

class CustomFileExtractor(FileMetadataExtractor):
    """
    Main 'custom' file-level extractor class
    Inherits from metalad's FileMetadataExtractor class
    """

    def get_id(self) -> UUID:
        return UUID("448109b1-8ee6-4b1f-9da2-29f7938f1d0e")

    def get_version(self) -> str:
        return "0.0.1"

    def get_data_output_category(self) -> DataOutputCategory:
        return DataOutputCategory.IMMEDIATE

    def is_content_required(self) -> bool:
        return False
    
    def get_required_content(self):
        # Instantiate CustomFileMetadata object
        file_metadata_obj = CustomFileMetadata(self.dataset, self.file_info)
        # Get required metadata file
        yield from file_metadata_obj.get_metafile()
        return True
    
    def extract(self, _=None) -> ExtractorResult:
        # Instantiate CustomFileMetadata object
        file_metadata_obj = CustomFileMetadata(self.dataset, self.file_info)
        # Extract metadata
        res = next(file_metadata_obj.get_metadata())
        return ExtractorResult(
            extractor_version=self.get_version(),
            extraction_parameter=self.parameter or {},
            extraction_success=True if res["status"] == "ok" else False,
            datalad_result_dict=res,
            immediate_data=res["metadata"],
        )


class CustomFileMetadata(object):
    """
    Util class to get custom file-level metadata from json file
    """

    def __init__(self, dataset, file_info) -> None:
        self.dataset = dataset
        self.file_info = file_info
        self.metafile_expression = self.get_metafile_expression()
        self.metafile_path = self.get_metafile_objpath()

    def get_metafile_expression(self):
        """Obtain configured custom content source

        Defaults to the expression '.metadata/content/{freldir}/{fname}.json'
        """
        return self.dataset.config.obtain(
            "datalad.metadata.custom-content-source",
            ".metadata/content/{freldir}/{fname}.json",
        )

    def get_metafile(self):
        """Ensure that the file with to-be-extracted metadata is
        locally available before extraction
        """
        if self.metafile_path.exists() or self.metafile_path.is_symlink():
            result = self.dataset.get(self.metafile_path, result_renderer="disabled")
            if result[0]["status"] in ("error", "impossible"):
                yield dict(
                    path=str(self.metafile_path),
                    action="meta_extract",
                    type="file",
                    status="error",
                    message=(
                        "required json file not retrievable: %s",
                        self.metafile_path,
                    ),
                )
            else:
                yield dict(
                    path=str(self.metafile_path),
                    action="meta_extract",
                    type="file",
                    status="ok",
                    message=("required json file retrieved"),
                )
        else:
            yield dict(
                path=str(self.metafile_path),
                action="meta_extract",
                type="file",
                status="impossible",
                message=(
                    "custom metadata source is not " "available at %s",
                    self.metafile_path,
                ),
            )

    def get_metafile_objpath(self) -> Path:
        """Get the path of the metadata file given the configured expression"""
        fpath = Path(self.file_info.path)
        # build associated metadata file path from POSIX
        # pieces and convert to platform conventions at the end
        return self.dataset.pathobj / PurePosixPath(
            self.metafile_expression.format(
                freldir=fpath.relative_to(self.dataset.pathobj).parent.as_posix(),
                fname=fpath.name,
            )
        )

    def get_metadata(self):
        """
        Function to load custom metadata from specified
        or default source(s)
        """
        log_progress(
            lgr.info,
            "extractorcustomfile",
            "Starting custom metadata extraction from file: {path}".format(
                path=str(self.metafile_path)
            ),
            total=1,
            label="custom metadata extraction",
            unit=" Files",
        )
        file_meta = {}
        lgr.debug("Load custom metadata from %s", self.metafile_path)
        try:
            meta = jsonload(str(self.metafile_path))
            file_meta.update(meta)
            log_progress(
                lgr.info,
                "extractorcustomfile",
                "Finished custom metadata extraction from file: {path}".format(
                    path=str(self.metafile_path)
                ),
            )
            yield dict(
                path=str(self.metafile_path),
                action="meta_extract",
                type="file",
                status="ok",
                message=("metadata extracted from file"),
                metadata=file_meta,
            )
        except Exception as e:
            ce = CapturedException(e)
            yield get_status_dict(
                action="meta_extract",
                status="error",
                exception=ce,
                type="file",
                message=("cannot load data from target path: %s", ce),
                path=str(self.metafile_path),
                metadata={},
            )
