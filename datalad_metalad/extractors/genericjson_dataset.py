# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""A dataset-level extractor for generic JSON(-LD) metadata contained in a dataset

One or more source files with metadata can be specified as an
extraction argument via the 'metadata_source' parameter.
The content of these files must be a JSON object, and a metadata
dictionary is built by updating it with the content of the JSON
objects in the order in which they are given.

An example of a single source file specified via extraction arguments:
datalad meta-extract -d mydataset metalad_genericjson_dataset 'metadata_source' 'dataset_metadata.json'

An example of a list of source file specified via extraction arguments:
datalad meta-extract -d mydataset metalad_genericjson_dataset 'metadata_source' '["dataset_metadata1.json", "dataset_metadata2.json"]'

By default a single file is read if no extraction arguments
are provided: '.metadata/dataset.json'
"""

from datalad_metalad.extractors.base import (
    DatasetMetadataExtractor,
    DataOutputCategory,
    ExtractorResult,
)

from pathlib import Path
from uuid import UUID
import json
import logging

lgr = logging.getLogger("datalad.metadata.extractors.genericjson_dataset")
from datalad.log import log_progress
from datalad.support.json_py import load as jsonload
from datalad.utils import (
    ensure_list,
)
from typing import Generator


class GenericJsonDatasetExtractor(DatasetMetadataExtractor):
    """
    Generic JSON dataset-level extractor class
    Inherits from metalad's DatasetMetadataExtractor class
    """

    def get_id(self) -> UUID:
        return UUID("fcf869b1-009d-4a3d-9afb-016034cbc3ca")

    def get_version(self) -> str:
        return "0.0.1"

    def get_data_output_category(self) -> DataOutputCategory:
        return DataOutputCategory.IMMEDIATE

    def get_required_content(self) -> Generator:
        # Get source files for metadata
        srcfiles, cfg_srcfiles = _get_dsmeta_srcfiles(self.dataset, self.parameter)
        # Deal with no metadata sources
        if not srcfiles:
            yield dict(
                path=self.dataset.path,
                type="dataset",
                status="impossible",
                message=(
                    "no configured or default metadata sources " "available in %s",
                    self.dataset.path,
                ),
            )
        # Get file content for returned sources
        # If file exists or is a symlink, get file, and yield failure
        # result if get operation returns a failure
        # If file doesn't exist or isn't a symlink, yield failure
        for f in srcfiles:
            f_abs = self.dataset.pathobj / f
            if f_abs.exists() or f_abs.is_symlink():
                result = self.dataset.get(f_abs, result_renderer="disabled")
                failure_count = 0
                for res in result:
                    if res["status"] in ("error", "impossible"):
                        failure_count += 1
                if failure_count > 0:
                    yield dict(
                        path=self.dataset.path,
                        action="meta_extract",
                        type="dataset",
                        status="error",
                        message=("required file content not retrievable: %s", f),
                    )
                else:
                    yield dict(
                        path=self.dataset.path,
                        action="meta_extract",
                        type="dataset",
                        status="ok",
                        message=("all required files retrieved"),
                    )
            else:
                yield dict(
                    path=self.dataset.path,
                    action="meta_extract",
                    type="dataset",
                    status="impossible",
                    message=(
                        "metadata source is not available in %s: %s",
                        self.dataset.path,
                        f,
                    ),
                )
        return

    def extract(self, _=None) -> ExtractorResult:
        return ExtractorResult(
            extractor_version=self.get_version(),
            extraction_parameter=self.parameter or {},
            extraction_success=True,
            datalad_result_dict={"type": "dataset", "status": "ok"},
            immediate_data=GenericJsonDatasetMetadata(self.dataset, self.parameter).get_metadata(),
        )


class GenericJsonDatasetMetadata(object):
    """
    Util class to get custom metadata from json files
    """

    def __init__(self, dataset, parameter: dict) -> None:
        self.dataset = dataset
        self.parameter = parameter

    def get_metadata(self):
        """
        Function to load custom metadata from specified
        or default source(s)
        """
        srcfiles, cfg_srcfiles = _get_dsmeta_srcfiles(self.dataset, self.parameter)

        log_progress(
            lgr.info,
            "extractorgenericjsondataset",
            "Start generic json metadata extraction from {path}".format(
                path=self.dataset.path
            ),
            total=len(srcfiles),
            label="generic json metadata extraction",
            unit=" Files",
        )
        dsmeta = {}
        for srcfile in srcfiles:
            abssrcfile = self.dataset.pathobj / srcfile
            lgr.debug("Load generic json metadata from %s", abssrcfile)
            meta = jsonload(str(abssrcfile))
            dsmeta.update(meta)
            log_progress(
                lgr.info,
                "extractorgenericjsondataset",
                f"Extracted generic json metadata from {abssrcfile}",
                update=1,
                increment=True,
            )

        log_progress(
            lgr.info,
            "extractorgenericjsondataset",
            "Finished generic json metadata extraction from {path}".format(
                path=self.dataset.path
            ),
        )
        return dsmeta


def _get_dsmeta_srcfiles(ds, extraction_args: dict) -> tuple:
    """Get the list of files containing dataset-level metadata

    Parameters
    ----------
    ds : Dataset
      DataLad Dataset instance

    extraction_args : dict
     Extraction arguments that were provided to the meta-extract call

    Returns
    -------
    tuple(srcfiles, cfg_srcfiles)
      srcfiles is a list of all source files for metadata, including default
      if it exists

      cfg_srcfiles is a list of all configured source files for metadata,
      where configuration occurs as an extraction argument via the
      'metadata_source' parameter 

      If no sources have been configured, the default '.metadata/dataset.json'
      is returned. If no sources are configured AND no file or symlink exists
      for the default path, an empty list is returned. All paths in the returned
      lists are relative
    """
    # Get metadata source filenames from configuration
    if not extraction_args:
        meta_source = []
    else:
        meta_source = extraction_args.get('metadata_source', None)
        if meta_source:
            if isinstance(meta_source, str):
                try:
                    # assuming json serialized list
                    meta_source = json.loads(meta_source)
                except:
                    # assuming single string value
                    meta_source = [meta_source]
            else:
                meta_source = ensure_list(meta_source)
        else:
            meta_source = []

    cfg_srcfiles = ensure_list(meta_source)
    default_path = ds.pathobj / ".metadata" / "dataset.json"
    srcfiles = []
    if not cfg_srcfiles:
        if default_path.exists() or default_path.is_symlink():
            srcfiles = [".metadata/dataset.json"]
    else:
        srcfiles = cfg_srcfiles
    return srcfiles, cfg_srcfiles
