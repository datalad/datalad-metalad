import os
import subprocess
import sys
from argparse import ArgumentParser
from pathlib import PosixPath
from typing import List


argument_parser = ArgumentParser(description="Parallel recursive metadata extraction")
argument_parser.add_argument("--max-processes", type=int, default=20, help="maximum number of parallel processes")
argument_parser.add_argument("command", type=str, help="The command name")
argument_parser.add_argument("dataset_path", type=str, help="The dataset from which metadata should be extracted")
argument_parser.add_argument("metalad_arguments", nargs="*")


g_arguments = None


running_processes: List[subprocess.Popen] = list()


def ensure_process_limit(max_processes: int):
    while len(running_processes) >= max_processes:
        for index, p in enumerate(running_processes):
            if p.poll() is not None:
                del running_processes[index]
                break


def execute_command_line(command_line):
    ensure_process_limit(g_arguments.max_processes)
    p = subprocess.Popen(command_line)
    running_processes.append(p)


def extract_dataset(realm: str, dataset_path: str, metalad_arguments: List[str]):
    command_line = [
        "datalad", "meta-extract", "metalad_core_dataset", "-d", dataset_path, "-i", realm] + metalad_arguments
    print(f"extract_dataset: {command_line}")
    execute_command_line(command_line)


def extract_file(realm: str, dataset_path: str, file_path: str, metalad_arguments: List[str]):
    command_line = [
        "datalad", "meta-extract", "metalad_core_file", file_path, "-d", dataset_path, "-i", realm] + metalad_arguments
    print(f"extract_file: {command_line}")
    execute_command_line(command_line)


def get_top_level_entry(path: str) -> os.DirEntry:
    p_path = PosixPath(path).resolve()
    above_path = PosixPath("/".join(p_path.parts[:-1])).resolve()
    return tuple(filter(lambda entry: entry.name == p_path.parts[-1], os.scandir(str(above_path))))[0]


def extract_recursive(realm: str, dataset_entry: os.DirEntry, entry: os.DirEntry, metalad_arguments: List[str]):
    if entry.is_dir():
        child_entries = tuple(os.scandir(entry.path))
        is_dataset = len(tuple(filter(lambda entry: entry.name == ".datalad", child_entries))) == 1
        if is_dataset:
            extract_dataset(realm, entry.path, metalad_arguments)
            dataset_entry = entry
        for entry in child_entries:
            if entry.name in (".noannex",) or entry.name.startswith(".git") or entry.name.startswith(".datalad"):
                continue
            extract_recursive(realm, dataset_entry, entry, metalad_arguments)
    else:
        extract_file(realm, dataset_entry.path, entry.path[len(dataset_entry.path) + 1:], metalad_arguments)


def extract_iterative(entry: os.DirEntry, metalad_arguments: List[str]) -> int:
    realm = entry.path
    entries_to_process = [entry]
    while entries_to_process:
        current_entry = entries_to_process.pop(0)
        if current_entry.is_dir():
            child_entries = tuple(os.scandir(current_entry.path))
            is_dataset = len(tuple(filter(lambda entry: entry.name == ".datalad", child_entries))) == 1
            if is_dataset:
                extract_dataset(realm, current_entry.path, metalad_arguments)
            for entry in child_entries:
                if entry.name in (".datalad",) or entry.name.startswith(".git"):
                    continue
                entries_to_process.append(entry)
        else:
            extract_file(realm, current_entry.path, metalad_arguments)

    return 0


def main(argument_vector) -> int:
    global g_arguments

    g_arguments = argument_parser.parse_args(argument_vector)
    print(g_arguments)
    top_dir_entry = get_top_level_entry(g_arguments.dataset_path)
    extract_recursive(top_dir_entry.path, top_dir_entry, top_dir_entry, g_arguments.metalad_arguments)
    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
