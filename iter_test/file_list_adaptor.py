#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from hashlib import md5


argument_parser = argparse.ArgumentParser(
    prog='file_list_adaptor',
    description='Interpret iterator output the output to create a tabby-style file list',
)

argument_parser.add_argument(
    'url_root',
    nargs='?',
    help='automatically create a URL with the given root. If nothing is provided'
         'no URL will we emitted'
)


def main():
    arguments = argument_parser.parse_args()
    line = sys.stdin.readline()
    first_line = True
    while line:
        if first_line is True:
            first_line = False
            print('path[POSIX]\tsize[bytes]\tchecksum[md5]\turl')
        file_info = json.loads(line)
        if file_info['type'] != 'file':
            sys.stderr.write(f'ignoring record with type {file_info["type"]}\n')
            continue

        with open(file_info['path'], 'rb') as f:
            digest = hashlib.file_digest(f, 'md5')

        name = file_info['intra_dataset_path']
        print(
            f'{name}'
            f'\t{file_info["bytesize"]}'
            f'\t{digest.hexdigest()}'
            + (f'\t{arguments.url_root + name}' if arguments.url_root else '')
        )
        line = sys.stdin.readline()


if __name__ == '__main__':
    main()
