#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


argument_parser = argparse.ArgumentParser(
    prog='extract_dataset_adaptor',
    description='Create dataset extract command line from an iterator record',
)

argument_parser.add_argument(
    'extractor_name',
    help='extractor that should be used'
)


def main():
    arguments = argument_parser.parse_args()
    line = sys.stdin.readline()
    while line:
        file_info = json.loads(line)
        if file_info['type'] != 'dataset':
            sys.stderr.write(f'ignoring record with type {file_info["type"]}\n')
            continue

        fs_base_path = Path(file_info['fs_base_path'])
        cmd_line = [
            '-d', str(fs_base_path / file_info['dataset_path']),
            '-c', f'{{"dataset_version": "{file_info["dataset_version"]}"}}'
        ]
        cmd_line.extend(['--file-info', json.dumps(file_info)])
        cmd_line.append(arguments.extractor_name)
        sys.stdout.write('\n'.join(cmd_line) + '\n')
        line = sys.stdin.readline()


if __name__ == '__main__':
    main()
