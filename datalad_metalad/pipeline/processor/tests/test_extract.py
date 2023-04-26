# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test metadata pipeline extract adaptor"""

import tempfile

from datalad.api import meta_conduct

from ....tests.utils import create_dataset_proper


extract_metadata_configuration = {
    "provider": {
        "module": "datalad_metalad.pipeline.provider.datasettraverse",
        "class": "DatasetTraverser",
        "name": "traverser",
        "arguments": {}
    },
    "processors": [
        {
            "module": "datalad_metalad.pipeline.processor.extract",
            "class": "MetadataExtractor",
            "name": "extractor",
            "arguments": {}
        },
        {
            "module": "datalad_metalad.pipeline.processor.add",
            "class": "MetadataAdder",
            "name": "adder",
            "arguments": {}
        }
    ]
}


def test_extract_with_sub_datasets():
    with tempfile.TemporaryDirectory() as root_dataset_dir:
        create_dataset_proper(
            directory=root_dataset_dir,
            sub_dataset_names=["subdataset_0"]
        )

        meta_conduct(
            configuration=extract_metadata_configuration,
            arguments=[
                f"traverser.top_level_dir={root_dataset_dir}",
                f"traverser.item_type=both",
                "traverser.traverse_sub_datasets=True",
                "extractor.extractor_type=dataset",
                "extractor.extractor_name=metalad_core"
            ],
            processing_mode="sequential"
        )
