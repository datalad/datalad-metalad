#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


argument_parser = argparse.ArgumentParser(
    prog='extract_adaptor',
    description='Iterate over the elements of a dataset',
)

argument_parser.add_argument(
    'extractor_name',
    help='extractor that should be used'
)

argument_parser.add_argument(
    'traverser_output',
    help='output of the traverser (JSON-string)'
)


def main():
    arguments = argument_parser.parse_args()
    file_info = json.loads(arguments.traverser_output)
    fs_base_path = Path(file_info['fs_base_path'])
    cmd_line = ['datalad', 'meta-extract', '-d', str(fs_base_path / file_info['dataset_path'])]
    if file_info['type'] == 'file':
        cmd_line.extend(['--file-info', arguments.traverser_output])
    cmd_line.append(arguments.extractor_name)
    if file_info['type'] == 'file':
        cmd_line.append(file_info['path'])
    subprocess.run(cmd_line, check=True)


if __name__ == '__main__':
    main()
