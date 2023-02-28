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
    DatasetMetadataExtractor,
    DataOutputCategory,
    ExtractorResult,
)

from pathlib import Path
from uuid import UUID
import logging
lgr = logging.getLogger('datalad.metadata.extractors.custom_dataset')
from datalad.log import log_progress
from datalad.support.json_py import load as jsonload
from datalad.utils import (
    ensure_list,
)


class CustomDatasetExtractor(DatasetMetadataExtractor):
    """
    Main 'custom' dataset-level extractor class
    Inherits from metalad's DatasetMetadataExtractor class
    """

    def get_id(self) -> UUID:
        return UUID("fcf869b1-009d-4a3d-9afb-016034cbc3ca")

    def get_version(self) -> str:
        return "0.0.1"

    def get_data_output_category(self) -> DataOutputCategory:
        return DataOutputCategory.IMMEDIATE
    
    def get_required_content(self):
        srcfiles, cfg_srcfiles = _get_dsmeta_srcfiles(self.dataset)
        for f in srcfiles:
            f_abs = self.dataset.pathobj / f
            if f_abs.exists() or f_abs.is_symlink():
                yield self.dataset.get(f_abs)
            else:
                if f in cfg_srcfiles:
                    yield dict(
                        path=self.dataset.path,
                        type='dataset',
                        status='impossible',
                        message=(
                            'configured custom metadata source is not '
                            'available in %s: %s',
                            self.dataset.path, f),
                    )
    
    def extract(self, _=None) -> ExtractorResult:
        return ExtractorResult(
            extractor_version=self.get_version(),
            extraction_parameter=self.parameter or {},
            extraction_success=True,
            datalad_result_dict={"type": "dataset", "status": "ok"},
            immediate_data=CustomDatasetMetadata(self.dataset).get_metadata()
        )


class CustomDatasetMetadata(object):
    """
    Util class to get custom metadata from json files
    """
    def __init__(self, dataset) -> None:
        self.dataset = dataset

    def get_metadata(self):
        """
        Function to load custom metadata from specified
        or default source(s)
        """
        srcfiles, cfg_srcfiles = _get_dsmeta_srcfiles(self.dataset)

        log_progress(
            lgr.info,
            "extractorcustomdataset",
            "Start custom metadata extraction from {path}".format(
                path=self.dataset.path
            ),
            total=len(srcfiles),
            label="custom metadata extraction",
            unit=" Files",
        )
        dsmeta = {}
        for srcfile in srcfiles:
            abssrcfile = self.dataset.pathobj / srcfile
            lgr.debug('Load custom metadata from %s', abssrcfile)
            if not abssrcfile.exists() and srcfile in cfg_srcfiles:
                raise FileNotFoundError(str(abssrcfile))
                # yield dict(
                #     path=self.dataset.path,
                #     type='dataset',
                #     status='impossible',
                #     message=(
                #         'configured custom metadata source is not '
                #         'available in %s: %s',
                #         self.dataset.path, srcfile),
                # )
            meta = jsonload(str(abssrcfile))
            dsmeta.update(meta)
            log_progress(
                lgr.info,
                'extractorcustomdataset',
                f'Extracted custom metadata from {abssrcfile}',
                update=1,
                increment=True)        
        
        log_progress(
            lgr.info,
            'extractorcustomdataset',
            'Finished custom metadata extraction from {path}'.format(
                path=self.dataset.path
            ),
        )
        # yield dict(
        #     path=self.dataset.path,
        #     metadata=dsmeta,
        #     type='dataset',
        #     status='ok',
        # )
        return dsmeta


def _get_dsmeta_srcfiles(ds):
    """Get the list of files containing dataset-level metadata
    """
    # Get metadata source filenames from configuration
    cfg_srcfiles = ds.config.obtain(
        'datalad.metadata.custom-dataset-source',
        [])
    cfg_srcfiles = ensure_list(cfg_srcfiles)
    # OK to be always POSIX
    default_path = ds.pathobj / '.metadata' / 'dataset.json'
    srcfiles = ['.metadata/dataset.json'] \
        if not cfg_srcfiles and (default_path.exists()
                                 or default_path.is_symlink()) \
        else cfg_srcfiles
    return srcfiles, cfg_srcfiles