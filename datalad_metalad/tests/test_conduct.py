# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
import json
import tempfile
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Dict

from datalad.api import meta_conduct
from datalad.tests.utils_pytest import (
    assert_equal,
    assert_true,
    eq_,
)

from .utils import create_dataset_proper
from datalad_metalad.pipeline.pipelinedata import (
    PipelineData,
    PipelineResult,
    ResultState,
)
from ..pipeline.processor.base import Processor
from ..pipeline.provider.base import Provider


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

test_provider = {
    "name": "testprovider",
    "module": "datalad_metalad.tests.test_conduct",
    "class": "TestTraverser",
    "arguments": {}
}


extract_pipeline = {
    "provider": {
        "name": "provider",
        "module": "datalad_metalad.pipeline.provider.datasettraverse",
        "class": "DatasetTraverser",
        "arguments": {}
    },
    "processors": [
        {
            "name": "testproc1",
            "module": "datalad_metalad.pipeline.processor.extract",
            "class": "MetadataExtractor",
            "arguments": {}
        },
        {
            "name": "testproc2",
            "module": "datalad_metalad.pipeline.processor.extract",
            "class": "MetadataExtractor",
            "arguments": {}
        }
    ]
}


@dataclass
class TestResult(PipelineResult):
    path: Path


@dataclass
class StringResult(PipelineResult):
    content: str

    def to_json(self) -> Dict:
        return {
            **super().to_json(),
            "content": self.content
        }


class TestTraverser(Provider):
    def __init__(self, path_spec: str):
        super().__init__()
        self.paths = [
            Path(path)
            for path in path_spec.split(":")]

    def next_object(self):
        for path in self.paths:
            yield PipelineData((
                ("path", path),
                (
                    "test-traversal-record",
                    [TestResult(ResultState.SUCCESS, path)]
                )
            ),)


class PathEater(Processor):
    def __init__(self):
        super().__init__()

    def process(self, pipeline_data: PipelineData) -> PipelineData:
        for ttr in pipeline_data.get_result("test-traversal-record"):
            ttr.path = Path().joinpath(*(ttr.path.parts[1:]))
        return pipeline_data


class DataAdder(Processor):
    """
    Add a StringPipelineResult with the string
    `data` and the
    """
    def __init__(self, source_name: str, content: str):
        super().__init__()
        self.source_name = source_name
        self.content = content

    def process(self, pipeline_data: PipelineData) -> PipelineData:
        pipeline_data.add_result(
            self.source_name,
            StringResult(ResultState.SUCCESS, self.content)
        )
        return pipeline_data


def test_simple_pipeline():
    simple_pipeline = {
        "provider": test_provider,
        "processors": [
            {
                "name": f"testproc{index}",
                "module": "datalad_metalad.tests.test_conduct",
                "class": "PathEater",
                "arguments": {}
            }
            for index in range(3)
        ]
    }

    pipeline_results = list(
        meta_conduct(
            arguments=[f"testprovider.path_spec=a/b/c:/d/e/f:/a/b/x:a/b/y"],
            configuration=simple_pipeline))

    eq_(len(pipeline_results), 4)

    # check for correct json encoding
    assert_true(all(map(json.dumps, pipeline_results)))


def test_extract():
    import logging
    logging.basicConfig(level=logging.DEBUG)

    with tempfile.TemporaryDirectory() as root_dataset_dir_str:
        create_dataset_proper(
            root_dataset_dir_str,
            ["subdataset_0", "subdataset_1"])

        pipeline_results = list(
            meta_conduct(
                arguments=[
                    f"provider.top_level_dir={root_dataset_dir_str}",
                    f"provider.item_type=both",
                    f"provider.traverse_sub_datasets=True",
                    f"testproc1.extractor_type=dataset",
                    f"testproc1.extractor_name=metalad_example_dataset",
                    f"testproc2.extractor_type=dataset",
                    f"testproc2.extractor_name=metalad_core"],
                configuration=extract_pipeline))

        eq_(len(pipeline_results), 3)
        assert_true(all(map(lambda e: e["status"] == "ok", pipeline_results)))

        # Ensure that each pipeline data carries two metadata results,
        # one from each extractor in the pipeline definition.
        assert_true(all(map(lambda e: len(e["pipeline_data"]["result"]["metadata"]) == 2, pipeline_results)))


def test_multiple_adder():
    adder_pipeline = {
        "provider": test_provider,
        "processors": [
            {
                "name": f"adder{index}",
                "module": "datalad_metalad.tests.test_conduct",
                "class": "DataAdder",
                "arguments": {}
            }
            for index in range(3)
        ]
    }

    adder_count = 3
    source_name = "adder-data"
    pipeline_results = list(
        meta_conduct(
            arguments=[
                "testprovider.path_spec=a/b/c",
                *list(chain(
                    *[
                        [
                            f"adder{index}.source_name={source_name}",
                            f"adder{index}.content=content from adder {index}"
                        ]
                        for index in range(adder_count)
                    ]
                ))
            ],
            configuration=adder_pipeline))

    eq_(len(pipeline_results), 1)
    result = pipeline_results[0]
    assert_equal(result["status"], "ok")
    pipeline_data = result["pipeline_data"]
    adder_results = pipeline_data["result"]["adder-data"]
    assert_equal(len(adder_results), adder_count)
    for i in range(adder_count):
        assert_equal(adder_results[i]["content"], f"content from adder {i}")
