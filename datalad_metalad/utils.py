from __future__ import annotations

import json
import pkg_resources
import sys
from itertools import islice
from pathlib import Path
from typing import Dict, List, Union

from datalad.distribution.dataset import (
    Dataset,
    require_dataset,
)
from datalad.runner import GitRunner
from datalad.runner.coreprotocols import StdOutErrCapture
from datalad.runner.exception import CommandError
from datalad.support.exceptions import NoDatasetFound

from .exceptions import NoDatasetIdFound
from .metadatatypes import JSONType


def args_to_dict(args: List[str]) -> Dict[str, str]:
    """ Convert an argument list to a dictionary """

    if args is None:
        return {}

    if len(args) % 2 != 0:
        raise ValueError(
            f"argument list is missing value for key '{args[-1]}'")

    return dict(
        zip(
            islice(args, 0, len(args), 2),
            islice(args, 1, len(args), 2)))


def error_result(action: str,
                 message: str,
                 status: str = "error"
                 ) -> dict:
    return dict(
        action=action,
        status=status,
        message=message)


def check_dataset(dataset_or_path: Union[Dataset, str],
                  purpose: str
                  ) -> Dataset:

    if isinstance(dataset_or_path, Dataset):
        dataset = dataset_or_path
    else:
        try:
            dataset = require_dataset(
                dataset_or_path,
                purpose=purpose,
                check_installed=True)
        except ValueError as ve:
            # This except clause translates datalad version 0.15 exceptions to
            # datalad version 0.16 exceptions
            if ve.args and ve.args[0].startswith("No installed dataset found "):
                raise NoDatasetFound(
                    "No valid datalad dataset found at: "
                    f"{Path(dataset_or_path).absolute()}")
            else:
                raise

    if not dataset.repo:
        raise NoDatasetFound(
            "No valid datalad dataset found at: "
            f"{dataset.path}")

    if not dataset.config.get("datalad.dataset.id"):
        raise NoDatasetIdFound(
            "No dataset id found in dataset at: "
            f"{dataset.path}")

    return dataset


def read_json_object(path_or_object: Union[str, JSONType]) -> JSONType:
    if isinstance(path_or_object, str):
        if path_or_object == "-":
            metadata_file = sys.stdin
        else:
            try:
                json_object = json.loads(
                    pkg_resources.resource_string(
                        "datalad_metalad.pipeline",
                        f"pipelines/{path_or_object}_pipeline.json"))
                return json_object
            except FileNotFoundError:
                metadata_file = open(path_or_object, "tr")
        return json.load(metadata_file)
    return path_or_object


def read_json_objects(path_or_object: Union[str, JSONType],
                      json_lines: bool
                      ) -> List[JSONType]:

    if isinstance(path_or_object, str):
        if path_or_object == "-":
            metadata_file = sys.stdin
        else:
            metadata_file = open(path_or_object, "tr")
        if json_lines is True:
            path_or_object = [
                json.loads(line)
                for line in metadata_file.readlines()
            ]
        else:
            path_or_object = json.load(metadata_file)

    return (
        path_or_object
        if isinstance(path_or_object, list)
        else [path_or_object])


def ls_struct(dataset: Dataset,
              paths: list[Path] | None = None
              ) -> dict[Path, dict]:

    flag_2_type = {
        "100644": "file",
        "100755": "file",
        "120000": "symlink",
        "160000": "dataset",
    }

    tag_2_status = {
        "C": "modified",
        "H": "clean",
    }

    path_args = list(map(str, paths)) if paths else []
    git_path_args = ["--"] + path_args if path_args else []

    runner = GitRunner()
    git_files = runner.run(
        ["git", "ls-files", "-s", "-m", "-t", "--exclude-standard"] + git_path_args,
        protocol=StdOutErrCapture,
        cwd=dataset.repo.pathobj
    )
    git_tree = runner.run(
        ["git", "ls-tree", "--full-tree", "--format=%(objectsize)%x09%(path)", "-r", "HEAD"] + path_args,
        protocol=StdOutErrCapture,
        cwd=dataset.repo.pathobj
    )
    try:
        annexed_here_out = runner.run(
            ["git", "annex", "find", "--format=${key} ${bytesize} ${file}\n"] + path_args,
            protocol=StdOutErrCapture,
            cwd=dataset.repo.pathobj
        )
        annexed_not_here_out = runner.run(
            ["git", "annex", "find", "--not", "--in", "here", "--format=¼{key} ${bytesize} ${file}\n"] + path_args,
            protocol=StdOutErrCapture,
            cwd=dataset.repo.pathobj
        )
        annexed_here = {
            line.split(maxsplit=2)[2]: line.split(maxsplit=2)[:2]
            for line in annexed_here_out["stdout"].splitlines()
            if line
        }
        annexed_not_here = {
            line.split(maxsplit=2)[2]: line.split(maxsplit=2)[:2]
            for line in annexed_not_here_out["stdout"].splitlines()
            if line
        }
        annexed = {
            **annexed_here,
            **annexed_not_here
        }
    except CommandError:
        annexed = set()

    size_info = {
        line.split(maxsplit=1)[1]: line.split(maxsplit=1)[0]
        for line in git_tree["stdout"].splitlines()
        if line.split(maxsplit=1)[1] not in annexed
    }
    result = dict()
    for line in git_files["stdout"].splitlines():
        line, path = line.split("\t", maxsplit=1)
        tag, flag, shasum, number = line.split()
        full_path = dataset.repo.pathobj / path
        if path in annexed:
            result[full_path] = {
                "type": "file",
                "path": str(full_path),
                "gitshasum": shasum,
                "state": tag_2_status[tag],
                "annexed": True,
                "key": annexed[path][0],
                "bytesize": int(annexed[path][1]),
                "has_content": path in annexed_here
            }
        else:
            result[full_path] = {
                "type": flag_2_type[flag],
                "path": str(full_path),
                "gitshasum": shasum,
                "state": tag_2_status[tag],
                "annexed": False,
                "bytesize": 0 if size_info[path] == "-" else int(size_info[path]),
                "has_content": False
            }

    return result
