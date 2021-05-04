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
from pathlib import Path
from typing import Optional, Union
from unittest.mock import patch
from uuid import UUID

from datalad.api import meta_conduct
from datalad.tests.utils import (
    with_tempfile,
    with_tree,
    assert_result_count,
    assert_true,
    assert_raises,
    assert_repo_status,
    eq_,
    known_failure,
)

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

    def process(self, path: Path) -> Path:
        if path.parts:
            return Path().joinpath(*(path.parts[1:]))
        return path

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

    configuration["provider"]["arguments"].append(dataset)
    result = list(meta_conduct(dataset=dataset, configuration=configuration))
    print(result)
