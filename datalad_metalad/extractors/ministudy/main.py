# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Metadata extractor for ministudy metadata contained in a dataset

The metadata source file can be specified via the
'datalad.metadata.ministudy-source' configuration variable.
The content of the file must be a JSON object that conforms to the
ministudy metadata schema. It can be serialized in JSON or in YAML.
Discrimination is performed on basis of the file suffix, i.e.
"json" for JSON and "yaml" for YAML.

By default the following file is read: '.metadata/ministudy.yaml'
"""

import logging

import yaml

from datalad_metalad.extractors.base import MetadataExtractor
from datalad_metalad.extractors.ministudy.ld_creator import LDCreator
from datalad.log import log_progress
from datalad.support.json_py import load as jsonload
from datalad.dochelpers import exc_str
from datalad.utils import Path, PurePosixPath, assure_list



lgr = logging.getLogger('datalad.metadata.extractors.ministudy')


class MiniStudyExtractor(MetadataExtractor):
    def get_required_content(self, dataset, process_type, status):
        for processed_status in status:
            if processed_status['path'].endswith('ministudy.yaml'):
                yield processed_status

    @staticmethod
    def _get_ministudy_srcfile(dataset) -> str:
        return dataset.config.obtain(
            'datalad.metadata.ministudy-source',
            str(dataset.pathobj / '.metadata' / 'ministudy.yaml'))

    def __call__(self, dataset, refcommit, process_type, status):
        ds = dataset
        log_progress(
            lgr.info,
            'extractorministudy',
            f'Start ministudy metadata extraction from {ds.path}',
            total=len(tuple(status)) + 1,
            label='Ministudy metadata extraction',
            unit=' Files',
        )

        source_file = self._get_ministudy_srcfile(dataset)
        with open(source_file, "rt") as input_stream:
            metadata_object = yaml.safe_load(input_stream)

        ld_creator_result = LDCreator(
            f"datalad://{dataset.id}", ",metadata/ministudy.yaml").create_ld_from_spec(metadata_object)

        yield {
            "status": "ok",
            "metadata": ld_creator_result.json_ld_object,
            "type": "dataset",
        }

        log_progress(
            lgr.info,
            'extractorministudy',
            f'Finished ministudy metadata extraction from {ds.path}'
        )

    def get_state(self, dataset):
        return {"version": "0.1"}
