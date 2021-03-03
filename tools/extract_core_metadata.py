import logging
import os
import re
import subprocess
import sys
from argparse import ArgumentParser, Namespace
from pathlib import PosixPath
from typing import List


log_level = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "fatal": logging.FATAL
}


argument_parser = ArgumentParser(description="Parallel recursive metadata extraction")
argument_parser.add_argument("--max-processes", type=int, default=20, help="maximum number of parallel processes")
argument_parser.add_argument("-l", "--log-level", type=str, default="warning", help="maximum number of parallel processes")
argument_parser.add_argument("command", type=str, help="The command name")
argument_parser.add_argument("dataset_path", type=str, help="The dataset from which metadata should be extracted")
argument_parser.add_argument("metalad_arguments", nargs="*")


logger = logging.getLogger("extract_core_metadata")


ignore_patterns = [
    re.compile(r"\.git.*"),
    re.compile(r"\.annex.*"),
    re.compile(r"\.noannex"),
    re.compile(r"\.datalad.*")
]


arguments: Namespace = argument_parser.parse_args(sys.argv)

running_processes: List[subprocess.Popen] = list()


def ensure_process_limit(max_processes: int):
    while len(running_processes) > max_processes:
        for index, p in enumerate(running_processes):
            if p.poll() is not None:
                logger.info(f"process {p.pid} exited")
                del running_processes[index]
                break


def execute_command_line(purpose, command_line):
    ensure_process_limit(arguments.max_processes)
    p = subprocess.Popen(command_line)
    running_processes.append(p)
    logger.info(f"started process {p.pid} to: {purpose}")


def extract_dataset(realm: str, dataset_path: str, metalad_arguments: List[str]):
    purpose = f"extract_dataset: {dataset_path}"
    command_line = [
        "datalad", "meta-extract", "metalad_core_dataset", "-d", dataset_path, "-i", realm] + metalad_arguments
    execute_command_line(purpose, command_line)


def extract_file(realm: str, dataset_path: str, file_path: str, metalad_arguments: List[str]):
    purpose = f"extract_file: {dataset_path}:{file_path}"
    command_line = [
        "datalad", "meta-extract", "metalad_core_file", file_path, "-d", dataset_path, "-i", realm] + metalad_arguments
    execute_command_line(purpose, command_line)


def get_top_level_entry(path: str) -> os.DirEntry:
    p_path = PosixPath(path).resolve()
    above_path = PosixPath("/".join(p_path.parts[:-1])).resolve()
    return tuple(filter(lambda entry: entry.name == p_path.parts[-1], os.scandir(str(above_path))))[0]


def should_be_ignored(name: str) -> bool:
    for pattern in ignore_patterns:
        if pattern.match(name):
            return True
    return False


def extract_recursive(realm: str, dataset_entry: os.DirEntry, entry: os.DirEntry, metalad_arguments: List[str]):
    if entry.is_dir():
        child_entries = tuple(os.scandir(entry.path))
        is_dataset = len(tuple(filter(lambda entry: entry.name == ".datalad", child_entries))) == 1
        if is_dataset:
            extract_dataset(realm, entry.path, metalad_arguments)
            dataset_entry = entry
        for entry in child_entries:
            if should_be_ignored(entry.name):
                continue
            extract_recursive(realm, dataset_entry, entry, metalad_arguments)
    else:
        extract_file(realm, dataset_entry.path, entry.path[len(dataset_entry.path) + 1:], metalad_arguments)


def main() -> int:
    logging.basicConfig(level=log_level[arguments.log_level])
    top_dir_entry = get_top_level_entry(arguments.dataset_path)
    extract_recursive(top_dir_entry.path, top_dir_entry, top_dir_entry, arguments.metalad_arguments)
    ensure_process_limit(0)
    return 0


if __name__ == "__main__":
    exit(main())
