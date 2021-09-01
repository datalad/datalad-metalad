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
import uuid
from pathlib import Path
from typing import List, Union

from datalad.api import meta_conduct
from datalad.tests.utils import (
    assert_equal,
    assert_in,
    with_tree,
    eq_,
)

from .utils import create_dataset
from ..processor.base import Processor
from ..provider.base import Provider


test_tree = {
    "a_0": {
        "b_0.0": {
            "c_0.0.0": "content",
            "c_0.0.1": "content"
        },
        "b_0.1": {
            "c_0.1.0": "content",
            "c_0.1.1": "content"
        }
    }
}

simple_pipeline = {
    "provider": {
        "name": "testprovider",
        "module": "datalad_metalad.tests.test_conduct",
        "class": "FilesystemTraverser",
        "arguments": [],
        "keyword_arguments": {}
    },
    "processors": [
        {
            "name": "testproc1",
            "module": "datalad_metalad.tests.test_conduct",
            "class": "PathEater",
            "arguments": [],
            "keyword_arguments": {}
        },
        {
            "name": "testproc2",
            "module": "datalad_metalad.tests.test_conduct",
            "class": "PathEater",
            "arguments": [],
            "keyword_arguments": {}
        },
        {
            "name": "testproc3",
            "module": "datalad_metalad.tests.test_conduct",
            "class": "PathEater",
            "arguments": [],
            "keyword_arguments": {}
        }
    ]
}


class FilesystemTraverser(Provider):
    def __init__(self, file_system_object: Union[str, Path]):
        super().__init__(file_system_object)
        self.file_system_objects = [Path(file_system_object)]

    def next_object(self):
        while self.file_system_objects:
            file_system_object = self.file_system_objects.pop()
            if file_system_object.is_symlink():
                continue
            elif file_system_object.is_dir():
                self.file_system_objects.extend(
                    list(file_system_object.glob("*")))
            else:
                yield file_system_object

    @staticmethod
    def output_type() -> str:
        return "pathlib.Path"


class PathEater(Processor):
    def __init__(self):
        super().__init__()

    def process(self, path: Path) -> List[Path]:
        if path.parts:
            return [Path().joinpath(*(path.parts[1:]))]
        return [path]

    @staticmethod
    def input_type() -> str:
        return "pathlib.Path"

    @staticmethod
    def output_type() -> str:
        return "pathlib.Path"


@with_tree(test_tree)
def test_simple_pipeline(dataset):

    pipeline_results = list(
        meta_conduct(
            arguments=[f"testprovider:{dataset}"],
            configuration=simple_pipeline))

    eq_(len(pipeline_results), 1)
    assert_equal(pipeline_results[0]["status"], "ended")


def test_extract():

    extract_pipeline = {
        "provider": {
            "name": "provider",
            "module": "datalad_metalad.provider.datasettraverse",
            "class": "DatasetTraverser",
            "arguments": [],
            "keyword_arguments": {}
        },
        "processors": [
            {
                "name": "testproc1",
                "module": "datalad_metalad.processor.extract",
                "class": "MetadataExtractor",
                "arguments": [],
                "keyword_arguments": {}
            }
        ]
    }

    import logging
    logging.basicConfig(level=logging.DEBUG)

    with tempfile.TemporaryDirectory() as root_dataset_dir_str:
        root_dataset_dir = Path(root_dataset_dir_str)
        subdataset_0_dir = root_dataset_dir / "subdataset_0"
        subdataset_1_dir = root_dataset_dir / "subdataset_1"

        create_dataset(root_dataset_dir, uuid.uuid4())
        create_dataset(subdataset_0_dir, uuid.uuid4())
        create_dataset(subdataset_1_dir, uuid.uuid4())

        pipeline_results = list(
            meta_conduct(
                arguments=[
                    f"provider:{root_dataset_dir_str}",
                    f"testproc1:Dataset",
                    f"testproc1:metalad_core_dataset"],
                configuration=extract_pipeline))

        eq_(len(pipeline_results), 1)
        assert_equal(pipeline_results[0]["status"], "ended")
