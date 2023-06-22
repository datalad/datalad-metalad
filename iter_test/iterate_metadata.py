#!/usr/bin/env python3

from __future__ import annotations

import json
from argparse import ArgumentParser

from datalad.api import meta_dump
from datalad_metalad.pipeline.provider.datasettraverse import DatasetTraverser
from datalad_metalad.pipeline.pipelinedata import PipelineDataState, ResultState


argument_parser = ArgumentParser(
    prog='iterate_metadata',
    description='Iterate over all metadata in a dataset',
)

argument_parser.add_argument(
    'dataset',
    help='directory that contains a datalad dataset with metadata'
)

argument_parser.add_argument(
    'metadata_path',
    nargs='?',
    help='forwarded to `path` argument of meta_dump()',
    default=""
)


def main():
    arguments = argument_parser.parse_args()
    for result in meta_dump(dataset=arguments.dataset,
                            path=arguments.metadata_path,
                            recursive=True,
                            return_type='generator',
                            result_renderer='disabled'):
        print(json.dumps(result, default=str))


if __name__ == '__main__':
    main()
