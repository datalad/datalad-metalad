# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
import tempfile
from pathlib import Path
from typing import cast
from uuid import UUID

from datalad.tests.utils import eq_

from ..extract import (
    MetadataExtractor,
    MetadataExtractorResult,
)
from ...pipelineelement import (
    PipelineElement,
    ResultState,
)
from ...provider.datasettraverse import DatasetTraverseResult
from ...tests.utils import create_dataset_proper


def test_basic_extract():
    with tempfile.TemporaryDirectory() as root_dataset_dir_str:
        ds = create_dataset_proper(root_dataset_dir_str)

        extractor = MetadataExtractor("Dataset", "metalad_core_dataset")

        pipeline_element = PipelineElement((
            ("path", root_dataset_dir_str),
            (
                "dataset-traversal-record",
                [
                    DatasetTraverseResult(**{
                        "state": ResultState.SUCCESS,
                        "fs_base_path": root_dataset_dir_str,
                        "type": "Dataset",
                        "path": "",
                        "dataset_path": Path("."),
                        "dataset_id": "012",
                        "dataset_version": "345"
                    })
                ]
            )))

        result = extractor.process(pipeline_element).get_result("metadata")
        eq_(len(result), 1)
        eq_(
            cast(
                MetadataExtractorResult,
                result[0]
            ).metadata_record["dataset_id"], UUID(ds.id))
