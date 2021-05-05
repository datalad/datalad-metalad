# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
from pathlib import Path
from typing import List, Union

from datalad.api import meta_conduct
from datalad.tests.utils import (
    assert_in,
    with_tree,
    eq_,
)

from ..conduct import ConductProcessorException
from ..processor.base import Processor
from ..provider.base import Provider


configuration = {
    "provider": {
        "module": "datalad_metalad.tests.test_conduct",
        "class": "FilesystemTraverser",
        "arguments": [],
        "keyword_arguments": {}
    },
    "processors": [
        {
            "module": "datalad_metalad.tests.test_conduct",
            "class": "PathEater",
            "arguments": [],
            "keyword_arguments": {}
        },
        {
            "module": "datalad_metalad.tests.test_conduct",
            "class": "PathEater",
            "arguments": [],
            "keyword_arguments": {}
        },
        {
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


@with_tree(
    {
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
    })
def test_simple_pipeline(dataset):

    pipeline_results = list(
        meta_conduct(
            arguments=[f"p:{dataset}"],
            configuration=configuration))

    eq_(len(pipeline_results), 4)

    result_paths = [str(result["result"]) for result in pipeline_results]
    assert_in("b_0.1/c_0.1.0", result_paths)
    assert_in("b_0.1/c_0.1.1", result_paths)
    assert_in("b_0.0/c_0.0.1", result_paths)
    assert_in("b_0.0/c_0.0.0", result_paths)
