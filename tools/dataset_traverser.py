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


from datalad_metalad.provider.datasettraverse import DatasetTraverser


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


def main():

    arguments = argument_parser.parse_args()
    t = DatasetTraverser(arguments.dataset_path, arguments.recursive)

    sys.stdout.write("[")
    first_line = True

    for pipeline_element in t.next_object():
        dtr_list = pipeline_element.get_result("dataset-traversal-record")
        for dtr in dtr_list:
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

            if first_line is False:
                sys.stdout.write(",")
            else:
                first_line = False
            sys.stdout.write("\n    " + json.dumps(datalad_result_dict))
    sys.stdout.write("\n]\n")


if __name__ == "__main__":
    exit(main())
