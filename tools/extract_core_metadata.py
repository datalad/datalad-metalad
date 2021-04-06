import logging
import os
import re
import subprocess
import sys
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Iterable, List


log_level = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "fatal": logging.FATAL
}

ignore_patterns = [
    re.compile(r"\.git.*"),
    re.compile(r"\.annex.*"),
    re.compile(r"\.noannex"),
    re.compile(r"\.datalad.*")
]

running_processes: List[subprocess.Popen] = list()


logger = logging.getLogger("extract_core_metadata")

argument_parser = ArgumentParser(
    description="Parallel recursive metadata extraction and storage")

argument_parser.add_argument(
    "-m", "--max-processes",
    type=int, default=20,
    help="maximum number of processes")

argument_parser.add_argument(
    "-l", "--log-level",
    type=str, default="warning",
    help="maximum number of parallel processes")

argument_parser.add_argument(
    "-r", "--recursive",
    action="store_true", default=False,
    help="collect metadata recursively in all datasets")

argument_parser.add_argument(
    "-a", "--aggregate",
    action="store_true", default=False,
    help="aggregate all extracted metadata in the top level dataset, this"
         "is only useful if recursive was specified (i.e. -r/--recursive)")

argument_parser.add_argument(
    "-f", "--file-extractor",
    type=str, default="metalad_core_file",
    help="file extractor name (default: metalad_core_file)")

argument_parser.add_argument(
    "-d", "--dataset-extractor",
    type=str, default="metalad_core_dataset",
    help="dataset extractor name (default: metalad_core_dataset)")

argument_parser.add_argument(
    "dataset_path",
    type=str,
    help="The dataset from which metadata should be extracted")

argument_parser.add_argument("metalad_arguments", nargs="*")


arguments: Namespace = argument_parser.parse_args(sys.argv[1:])


def ensure_less_processes_than(max_processes: int):
    while len(running_processes) >= max_processes:
        for index, p in enumerate(running_processes):
            if p.poll() is not None:
                logger.debug(f"process {p.pid} exited")
                del running_processes[index]
                break


def execute_command_line(purpose, command_line):
    ensure_less_processes_than(arguments.max_processes)
    p = subprocess.Popen(command_line)
    running_processes.append(p)
    logger.info(
        f"started process {p.pid} [{purpose}]: {' '.join(command_line)}")


def extract_dataset_level_metadata(metadata_store: str,
                                   dataset_path: str,
                                   metalad_arguments: List[str]):

    purpose = f"extract_dataset: {dataset_path}"
    command_line = [
        "datalad", "-l", arguments.log_level, "meta-extract",
        f"{arguments.dataset_extractor}", "-d", dataset_path,
        "-i", metadata_store
    ] + metalad_arguments
    execute_command_line(purpose, command_line)


def extract_file_level_metadata(realm: str,
                                dataset_path: str,
                                file_path: str,
                                metalad_arguments: List[str]):

    purpose = f"extract_file: {dataset_path}:{file_path}"
    command_line = [
        "datalad", "-l", arguments.log_level, "meta-extract",
        f"{arguments.file_extractor}", file_path, "-d",
        dataset_path, "-i", realm
    ] + metalad_arguments
    execute_command_line(purpose, command_line)


def get_top_level_entry(path: str) -> os.DirEntry:
    p_path = Path(path).resolve()
    parent_path = Path("/".join(p_path.parts[:-1])).resolve()
    return tuple(filter(
        lambda entry: entry.name == p_path.parts[-1], os.scandir(
            str(parent_path))))[0]


def should_be_ignored(name: str) -> bool:
    for pattern in ignore_patterns:
        if pattern.match(name):
            return True
    return False


def is_dataset(child_entries: Iterable[os.DirEntry]):
    return len(tuple(filter(
        lambda entry: entry.name == ".datalad", child_entries))) == 1


def extract_file_recursive(realm: str,
                           dataset_entry: os.DirEntry,
                           entry: os.DirEntry,
                           metalad_arguments: List[str]):
    if entry.is_dir():
        child_entries = tuple(os.scandir(entry.path))
        if is_dataset(child_entries):
            if dataset_entry.path != entry.path:
                return
        for entry in child_entries:
            if should_be_ignored(entry.name):
                continue
            extract_file_recursive(
                realm, dataset_entry, entry, metalad_arguments)
    else:
        extract_file_level_metadata(
            realm,
            dataset_entry.path,
            entry.path[len(dataset_entry.path) + 1:],
            metalad_arguments)


def extract_individual(dataset_entry: os.DirEntry,
                       metalad_arguments: List[str]):

    extract_dataset_level_metadata(
        dataset_entry.path,
        dataset_entry.path,
        metalad_arguments)

    extract_file_recursive(
        dataset_entry.path,
        dataset_entry,
        dataset_entry,
        metalad_arguments)


def extract_individual_recursive(realm: str,
                                 dataset_entry: os.DirEntry,
                                 entry: os.DirEntry,
                                 metalad_arguments: List[str]):

    if entry.is_dir():
        child_entries = tuple(os.scandir(entry.path))
        if is_dataset(child_entries):
            extract_individual(entry, metalad_arguments)
        for entry in child_entries:
            if should_be_ignored(entry.name):
                continue
            extract_individual_recursive(
                realm, dataset_entry, entry, metalad_arguments)


def extract_recursive(realm: str,
                      dataset_entry: os.DirEntry,
                      entry: os.DirEntry,
                      metalad_arguments: List[str]):

    if entry.is_dir():
        child_entries = tuple(os.scandir(entry.path))
        if is_dataset(child_entries):
            extract_dataset_level_metadata(realm, entry.path, metalad_arguments)
            dataset_entry = entry
        for entry in child_entries:
            if should_be_ignored(entry.name):
                continue
            extract_recursive(realm, dataset_entry, entry, metalad_arguments)
    else:
        extract_file_level_metadata(
            realm,
            dataset_entry.path,
            entry.path[len(dataset_entry.path) + 1:],
            metalad_arguments)


def main() -> int:
    logging.basicConfig(level=log_level[arguments.log_level])

    if arguments.max_processes < 1:
        print(
            "Error: number of processes must be greater or equal to 1",
            file=sys.stderr)
        return 1

    if arguments.aggregate is True:
        if arguments.recursive is False:
            print(
                "Warning: 'aggregate' ignored, since 'recursive' is not set",
                file=sys.stderr)
            arguments.into = None
        else:
            arguments.into = arguments.dataset_path
    else:
        arguments.into = None

    top_dir_entry = get_top_level_entry(arguments.dataset_path)
    if arguments.recursive is True:
        if arguments.into:
            extract_recursive(
                top_dir_entry.path,
                top_dir_entry,
                top_dir_entry,
                arguments.metalad_arguments)
        else:
            extract_individual_recursive(
                top_dir_entry.path,
                top_dir_entry,
                top_dir_entry,
                arguments.metalad_arguments)
    else:
        extract_individual(
            top_dir_entry,
            arguments.metalad_arguments)

    ensure_less_processes_than(1)
    return 0


if __name__ == "__main__":
    exit(main())
