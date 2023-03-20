# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""A file-level extractor for generic JSON(-LD) metadata

When presented with a file in a dataset, the Generic JSON file-level
extractor will extract metadata from a specified and related metadata
source file. A sidecar source file with metadata can be specified as an
extraction argument via the 'metadata_source' parameter. The metadata
source file name can be specified explicitly or via a parameterized
expression. The content of the source file must be a JSON object.

An example of an explicitly specified source file:
datalad meta-extract -d mydataset metalad_genericjson_file myfile.txt 'metadata_source' '_myfile_meta.json'

An example of a parameterized expression:
datalad meta-extract -d mydataset metalad_genericjson_file myfile.txt 'metadata_source' '{freldir}_{fname}_meta.json'

In the case of a parameterized expression, the parameters 'freldir'
and 'fname' are determined at runtime as the parent directory of the main
file and the name of the main file, respectively. These parameter names are
required and cannot be changed.

If no explicit source file or expression is provided, the default location
for a sidecar source file will be used. The default root directory is
'.metadata/content/' and the source file is then required to be located at
the same relative level of the filetree as the main file with content.
The filename of the source file should be identical to that of the main file,
and the extension should be '.json'.

An example of a source file in the default location is '.metadata/content/myfile.json',
for a main file specified as 'myfile.txt'.
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


class GenericJsonFileExtractor(FileMetadataExtractor):
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
        file_metadata_obj = GenericJsonFileMetadata(
            self.dataset, self.file_info, self.parameter)
        # Get required metadata file
        yield from file_metadata_obj.get_metafile()
        return True
    
    def extract(self, _=None) -> ExtractorResult:
        # Instantiate CustomFileMetadata object
        file_metadata_obj = GenericJsonFileMetadata(
            self.dataset, self.file_info, self.parameter)
        # Extract metadata
        res = next(file_metadata_obj.get_metadata())
        return ExtractorResult(
            extractor_version=self.get_version(),
            extraction_parameter=self.parameter or {},
            extraction_success=True if res["status"] == "ok" else False,
            datalad_result_dict=res,
            immediate_data=res["metadata"],
        )


class GenericJsonFileMetadata(object):
    """
    Util class to get custom file-level metadata from a sidecar json file
    """

    def __init__(self, dataset, file_info, parameter) -> None:
        self.dataset = dataset
        self.file_info = file_info
        self.extraction_args = parameter
        self.metafile_expression = self.get_metafile_expression()
        self.metafile_path = self.get_metafile_objpath()

    def get_metafile_expression(self):
        """Get configured custom metadata source from extraction arguments

        Defaults to the expression '.metadata/content/{freldir}/{fname}.json'
        """
        default_expr = ".metadata/content/{freldir}/{fname}.json"
        if not self.extraction_args:
            return default_expr
        else:
            return self.extraction_args.get('metadata_source', default_expr)
        
    def get_metafile_objpath(self) -> Path:
        """Get the path of the metadata file given by the configured expression"""
        fpath = Path(self.file_info.path)
        # build associated metadata file path from POSIX
        # pieces and convert to platform conventions at the end
        return self.dataset.pathobj / PurePosixPath(
            self.metafile_expression.format(
                freldir=fpath.relative_to(self.dataset.pathobj).parent.as_posix(),
                fname=fpath.name,
            )
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
