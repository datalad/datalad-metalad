#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
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


def main():
    arguments = argument_parser.parse_args()
    for line in sys.stdin.readlines():
        file_info = json.loads(line)
        fs_base_path = Path(file_info['fs_base_path'])
        cmd_line = [
            '-d', str(fs_base_path / file_info['dataset_path']),
            '-c', f'{{"dataset_version": "{file_info["dataset_version"]}"}}'
        ]
        if file_info['type'] == 'file':
            cmd_line.extend(['--file-info', json.dumps(file_info)])
        cmd_line.append(arguments.extractor_name)
        if file_info['type'] == 'file':
            cmd_line.append(file_info['path'])
        print ('\n'.join(cmd_line))


if __name__ == '__main__':
    main()
