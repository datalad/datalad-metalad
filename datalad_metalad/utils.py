import json
import pkg_resources
import sys
from itertools import islice
from typing import Dict, List, Union

from datalad.distribution.dataset import (
    Dataset,
    require_dataset,
)
from datalad.support.exceptions import NoDatasetFound

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
                    f"{dataset_or_path}")
            else:
                raise

    if not dataset.repo:
        raise NoDatasetFound(
            "No valid datalad dataset found at: "
            f"{dataset.path}")

    if dataset.id is None:
        raise NoDatasetFound(
            "No valid datalad-id found in dataset at: "
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
