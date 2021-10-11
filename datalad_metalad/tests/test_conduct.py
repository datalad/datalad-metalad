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
from dataclasses import dataclass
from itertools import chain
from pathlib import Path

from datalad.api import meta_conduct
from datalad.tests.utils import (
    assert_equal,
    eq_,
)

from .utils import create_dataset
from ..pipelineelement import (
    PipelineElement,
    PipelineResult,
    ResultState,
)
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


test_provider = {
    "name": "testprovider",
    "module": "datalad_metalad.tests.test_conduct",
    "class": "TestTraverser",
    "arguments": [],
    "keyword_arguments": {}
}


@dataclass
class TestResult(PipelineResult):
    path: Path


@dataclass
class StringResult(PipelineResult):
    content: str


class TestTraverser(Provider):
    def __init__(self, path_spec: str):
        super().__init__()
        self.paths = [
            Path(path)
            for path in path_spec.split(":")]

    def next_object(self):
        for path in self.paths:
            yield PipelineElement((
                ("path", path),
                (
                    "test-traversal-record",
                    [TestResult(ResultState.SUCCESS, path)]
                )
            ),)


class PathEater(Processor):
    def __init__(self):
        super().__init__()

    def process(self, pipeline_element: PipelineElement) -> PipelineElement:
        for ttr in pipeline_element.get_result("test-traversal-record"):
            ttr.path = Path().joinpath(*(ttr.path.parts[1:]))
        return pipeline_element


class DataAdder(Processor):
    """
    Add a StringPipelineResult with the string
    `data` and the
    """
    def __init__(self, source_name: str, content: str):
        super().__init__()
        self.source_name = source_name
        self.content = content

    def process(self, pipeline_element: PipelineElement) -> PipelineElement:
        pipeline_element.add_result(
            self.source_name,
            StringResult(ResultState.SUCCESS, self.content)
        )
        return pipeline_element


def test_simple_pipeline():
    simple_pipeline = {
        "provider": test_provider,
        "processors": [
            {
                "name": f"testproc{index}",
                "module": "datalad_metalad.tests.test_conduct",
                "class": "PathEater",
                "arguments": [],
                "keyword_arguments": {}
            }
            for index in range(3)
        ]
    }

    pipeline_results = list(
        meta_conduct(
            arguments=[f"testprovider:a/b/c:/d/e/f:/a/b/x:a/b/y"],
            configuration=simple_pipeline))

    eq_(len(pipeline_results), 4)


def test_extract():
    import logging
    logging.basicConfig(level=logging.DEBUG)

    with tempfile.TemporaryDirectory() as root_dataset_dir_str:
        root_dataset_dir = Path(root_dataset_dir_str)
        subdataset_0_dir = root_dataset_dir / "subdataset_0"
        subdataset_1_dir = root_dataset_dir / "subdataset_1"

        create_dataset(str(root_dataset_dir), uuid.uuid4())
        create_dataset(str(subdataset_0_dir), uuid.uuid4())
        create_dataset(str(subdataset_1_dir), uuid.uuid4())

        pipeline_results = list(
            meta_conduct(
                arguments=[
                    f"provider:{root_dataset_dir_str}",
                    f"provider:both",
                    f"testproc1:Dataset",
                    f"testproc1:metalad_core_dataset"],
                configuration=extract_pipeline))

        eq_(len(pipeline_results), 1)
        assert_equal(pipeline_results[0]["status"], "ok")


def test_multiple_adder():
    adder_pipeline = {
        "provider": test_provider,
        "processors": [
            {
                "name": f"adder{index}",
                "module": "datalad_metalad.tests.test_conduct",
                "class": "DataAdder",
                "arguments": [],
                "keyword_arguments": {}
            }
            for index in range(3)
        ]
    }

    adder_count = 3
    source_name = "adder-data"
    pipeline_results = list(
        meta_conduct(
            arguments=[
                "testprovider:a/b/c",
                *list(chain(*[
                    [f"adder{index}:{source_name}", f"adder{index}:content from adder {index}"]
                    for index in range(adder_count)
                ]))
            ],
            configuration=adder_pipeline))

    eq_(len(pipeline_results), 1)
    result = pipeline_results[0]
    assert_equal(result["status"], "ok")
    pipeline_element = result["pipeline_element"]
    adder_results = pipeline_element.get_result("adder-data")
    assert_equal(len(adder_results), adder_count)
    for i in range(adder_count):
        assert_equal(adder_results[i].content, f"content from adder {i}")
