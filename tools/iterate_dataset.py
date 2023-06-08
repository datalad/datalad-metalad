#!/usr/bin/env python3

from __future__ import annotations

import json
from argparse import ArgumentParser

from datalad_metalad.pipeline.provider.datasettraverse import DatasetTraverser
from datalad_metalad.pipeline.pipelinedata import PipelineData, PipelineDataState, ResultState


argument_parser = ArgumentParser(
    prog='iterate_dataset',
    description='Iterate over the elements of a dataset',
)

argument_parser.add_argument(
    'top_level_dir',
    help='directory that contains a datalad dataset'
)

argument_parser.add_argument(
    'item_type',
    default='both',
    help='type of the items that should be returned ("file", "dataset", or "both" [default])'
)

argument_parser.add_argument(
    'traverse_sub_datasets',
    default=False,
    type=bool,
    help='traverse subdatasets ("True" or "False" [default])'
)

def main():
    arguments = argument_parser.parse_args()
    traverser = DatasetTraverser(
        top_level_dir=arguments.top_level_dir,
        item_type=arguments.item_type,
        traverse_sub_datasets=arguments.traverse_sub_datasets
    )
    for element in traverser.next_object():
        if element.state == PipelineDataState.CONTINUE:
            for result in element.get_result('dataset-traversal-record'):
                if result.state == ResultState.SUCCESS:
                    output = result.to_dict()
                    for key in ('dataset_id', 'dataset_version'):
                        output['element_info'][key] = output[key]
                    print(json.dumps(output['element_info']))


if __name__ == '__main__':
    main()
