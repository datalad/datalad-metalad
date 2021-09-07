#!/usr/bin/env python
"""
This module traverses a dataset and can be
used with a CWL-runner
"""
import json
import sys
from argparse import (
    ArgumentParser,
)
from typing import Dict


from datalad_metalad.provider.datasettraverse import (
    DatasetTraverser,
    DatasetTraverseResult
)


argument_parser = ArgumentParser(
    description="Traverse a dataset and print an JSON array with entries "
                "for each file or subdataset element of the dataset."
)
argument_parser.add_argument(
    "dataset_path",
    type=str,
    help="path to the dataset that should be traversed"
)
argument_parser.add_argument(
    "-r", "--recursive",
    action="store_true",
    default=False,
    help="recurse into sub datasets (if they are installed)"
)


def get_dict_result(dtr: DatasetTraverseResult) -> str:
    datalad_result_dict = {
        "status": "ok",
        "type": "file" if dtr.type == "File" else "dataset",
        "path": str(dtr.path),
        "dataset_path": str(dtr.dataset_path),
        "dataset_id": str(dtr.dataset_id),
        "dataset_version": dtr.dataset_version
    }
    if dtr.root_dataset_id is not None and dtr.root_dataset_version is not None:
        datalad_result_dict["root_dataset_id"] = dtr.root_dataset_id
        datalad_result_dict["root_dataset_version"] = dtr.root_dataset_version

    return json.dumps(datalad_result_dict)


def emit_cwl_result(traverser: DatasetTraverser):
    sys.stdout.write("""{
    "type": ["string", "array"],
    "value": [
""")
    first_line = True
    for pipeline_element in traverser.next_object():
        dtr_list = pipeline_element.get_result("dataset-traversal-record")
        for dtr in dtr_list:
            if not first_line:
                sys.stdout.write(",\n")
            first_line = False
            sys.stdout.write(f'        "{str(dtr.path)}"')
    sys.stdout.write('\n    ]\n}\n')


def emit_cwl_complex_result(traverser: DatasetTraverser):
    sys.stdout.write("""{
    "type": ["Any", "array"],
    "value": [
""")
    first_line = True
    for pipeline_element in traverser.next_object():
        dtr_list = pipeline_element.get_result("dataset-traversal-record")
        for dtr in dtr_list:
            datalad_result_dict = {
                "status": "ok",
                "type": "file" if dtr.type == "File" else "dataset",
                "top_level_path": str(traverser.top_level_dir),
                "path": str(dtr.path),
                "dataset_path": str(dtr.dataset_path),
                "dataset_id": str(dtr.dataset_id),
                "dataset_version": dtr.dataset_version
            }
            if dtr.root_dataset_id is not None and dtr.root_dataset_version is not None:
                datalad_result_dict["root_dataset_id"] = dtr.root_dataset_id
                datalad_result_dict[
                    "root_dataset_version"] = dtr.root_dataset_version

            if not first_line:
                sys.stdout.write(",\n")
            first_line = False
            sys.stdout.write(f'        {json.dumps(datalad_result_dict)}')
    sys.stdout.write('\n    ]\n}\n')


def main():

    arguments = argument_parser.parse_args()
    t = DatasetTraverser(arguments.dataset_path, arguments.recursive)

    emit_cwl_complex_result(t)
    return 0


if __name__ == "__main__":
    exit(main())
