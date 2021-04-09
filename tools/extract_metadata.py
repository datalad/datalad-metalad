import json
import logging
import os
import re
import subprocess
import sys
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


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


logger = logging.getLogger("extract_core_metadata")


argument_parser = ArgumentParser(
    description="Parallel recursive metadata extraction and storage")

argument_parser.add_argument(
    "-p", "--process-limit",
    type=int, default=20,
    help="maximum number of parallel processes")

argument_parser.add_argument(
    "-l", "--log-level",
    type=str, default="warning",
    help="maximum number of parallel processes")

argument_parser.add_argument(
    "-r", "--recursive",
    action="store_true", default=False,
    help="collect metadata recursively in all datasets, metadata is"
         "stored in the repository of the respective dataset unless -a"
         "is specified")

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
    "-m", "--metadata-store",
    type=str,
    help="if -s, --save-metadata is given, store extracted metadata in "
         "the repository given here, instead of the root-dataset repository")

argument_parser.add_argument(
    "-s", "--save-metadata",
    action="store_true", default=False,
    help="save extracted metadata in the root-dataset repository (or "
         "the repository given by -m, --metadata-store) instead of "
         "writting records to stdout.")

argument_parser.add_argument(
    "dataset_path",
    type=str,
    help="The dataset from which metadata should be extracted")

argument_parser.add_argument("metalad_arguments", nargs="*")


arguments: Namespace = argument_parser.parse_args(sys.argv[1:])
print(arguments, file=sys.stderr)


@dataclass
class ExtractorInfo:
    popen: subprocess.Popen
    context_info: Dict
    metadata_store: Optional[Path]


running_processes: List[ExtractorInfo] = list()


def encapsulate(metadata_object: dict, context: dict) -> dict:
    return {
        "datalad_encapsulation": {
            "creator": "extract_metadata_orchestration",
            "encapsulated_data_creator": "meta_extract"
        },
        "encapsulated_data": metadata_object,
        "this_level_data": context
    }


def get_add_cmdline(metadata_store: Path,
                    context: Dict) -> List[str]:

    command_line = [
        "datalad",
        "-l", arguments.log_level,
        "meta-add",
        "-m", str(metadata_store),
        "-"]

    if "root_dataset_id" in context:
        additional_values = {
            key: value
            for key, value in context.items()
            if key in [
                "root_dataset_id",
                "root_dataset_version",
                "inter_dataset_path"]
        }
        command_line.append(json.dumps(additional_values))
    return command_line


def add_metadata(metadata_store: Path,
                 metadata_object: Dict,
                 context: Dict):

    command_line = get_add_cmdline(metadata_store, context)
    stdin_content = json.dumps(metadata_object).encode()
    logger.info(f"running: {' '.join(command_line)}")
    subprocess.run(command_line, input=stdin_content, check=True)


# TODO: use coroutines
def handle_process_termination():
    terminated_info = []
    for index, extractor_info in enumerate(running_processes):
        if extractor_info.popen.poll() is not None:
            logger.debug(f"process {extractor_info.popen.pid} exited")
            terminated_info.append(extractor_info)
            output, _ = extractor_info.popen.communicate()
            metadata_object = json.loads(output.decode())
            if extractor_info.metadata_store is None:
                json.dump(
                    encapsulate(metadata_object, extractor_info.context_info),
                    sys.stdout)
            else:
                add_metadata(
                    extractor_info.metadata_store,
                    metadata_object,
                    extractor_info.context_info)

    for extractor_info in terminated_info:
        running_processes.remove(extractor_info)


def ensure_less_processes_than(process_limit: int):
    while len(running_processes) >= process_limit:
        handle_process_termination()


def execute_command_line(purpose: str,
                         command_line: List[str],
                         context_info: Dict,
                         metadata_store: Optional[Path] = None):

    ensure_less_processes_than(arguments.process_limit)
    p = subprocess.Popen(command_line, stdout=subprocess.PIPE)
    running_processes.append(ExtractorInfo(p, context_info, metadata_store))
    logger.info(
        f"started process {p.pid} [{purpose}]: {' '.join(command_line)}")


def get_metadata_store(dataset_path: Path) -> Optional[Path]:
    if arguments.save_metadata is True:
        if arguments.metadata_store is not None:
            return Path(arguments.metadata_store)
        return dataset_path
    return None


def get_dataset_level_extract_cmdline(dataset_path: Path,
                                      metalad_arguments: List[str]
                                      ) -> List[str]:
    return [
        "datalad",
        "-l", arguments.log_level,
        "meta-extract",
        "-d", str(dataset_path),
        arguments.dataset_extractor,
    ] + (
        ["++"] + metalad_arguments
        if metalad_arguments
        else [])


def extract_dataset_level_metadata(dataset_path: Path,
                                   metalad_arguments: List[str],
                                   context_info: Dict):

    purpose = f"extract_dataset: {dataset_path}"
    command_line = get_dataset_level_extract_cmdline(
        dataset_path,
        metalad_arguments)
    execute_command_line(
        purpose,
        command_line,
        context_info,
        get_metadata_store(dataset_path))


def get_file_level_extract_cmdline(dataset_path: Path,
                                   file_path: Path,
                                   metalad_arguments: List[str]
                                   ) -> List[str]:
    return [
        "datalad",
        "-l", arguments.log_level,
        "meta-extract",
        "-d", str(dataset_path),
        arguments.file_extractor,
        str(file_path)
    ] + metalad_arguments


def extract_file_level_metadata(dataset_path: Path,
                                file_path: Path,
                                metalad_arguments: List[str],
                                context_info: Dict):

    purpose = f"extract_file: {dataset_path}:{file_path}"
    command_line = get_file_level_extract_cmdline(
        dataset_path,
        file_path,
        metalad_arguments)
    execute_command_line(
        purpose,
        command_line,
        context_info,
        get_metadata_store(dataset_path))


def should_be_ignored(name: str) -> bool:
    for pattern in ignore_patterns:
        if pattern.match(name):
            return True
    return False


def is_dataset(parent_path: Path) -> Tuple[Optional[str], Optional[str]]:
    child_entries = tuple(os.scandir(str(parent_path)))
    datalad_dir_entry = (tuple(filter(
        lambda entry: entry.name == ".datalad", child_entries)) + (None,))[0]

    if datalad_dir_entry is not None:
        try:
            dataset_id = subprocess.run([
                "git",
                "-P",
                "config",
                "-f",
                datalad_dir_entry.path + "/config",
                "datalad.dataset.id"],
                check=True,
                stdout=subprocess.PIPE
            ).stdout.decode().strip()

            dataset_version = subprocess.run([
                "git",
                "-P",
                "--git-dir",
                str(parent_path / ".git"),
                "log",
                "--pretty=%H",
                "HEAD...HEAD^1"],
                check=True,
                stdout=subprocess.PIPE
            ).stdout.decode().strip()
        except subprocess.CalledProcessError:
            return None, None
        return dataset_id, dataset_version

    return None, None


def extract_recursive(root_dataset_path: Path,
                      root_dataset_id: str,
                      root_dataset_version: str,
                      current_dataset_path: Path,
                      current_path: Path,
                      metalad_arguments: List[str],
                      dataset_recursive: bool,
                      aggregate: bool):

    """

    Parameters
    ----------
    root_dataset_path
    root_dataset_id
    root_dataset_version
    current_dataset_path
    current_path
    metalad_arguments
    dataset_recursive
    aggregate

    Returns
    -------
    None

    Determine which extractors should be run for dataset- and
    file-level metadata extraction. If `dataset_recursive` is
    `True`, extract will process datasets that are contained
    in the root dataset. If aggregate is given, the metadata
    records will be amended with sub-dataset information that
    allows it to be added as aggregated data in the root
    dataset.
    """
    # The following two lines assert preconditions
    _ = current_path.relative_to(current_dataset_path)
    _ = current_dataset_path.relative_to(root_dataset_path)

    if current_path.is_dir():
        dataset_id, dataset_version = is_dataset(current_path)
        if dataset_id is not None:
            if dataset_id == root_dataset_id:
                assert dataset_version == root_dataset_version
                assert current_dataset_path == root_dataset_path

                logger.debug(
                    f"Extract root dataset {root_dataset_path} "
                    f"to {root_dataset_path}[]")

                extract_dataset_level_metadata(
                    root_dataset_path,
                    metalad_arguments,
                    dict(
                        root_dataset_path=str(root_dataset_path)
                    ))
            else:
                if not dataset_recursive:
                    return

                inter_dataset_path = current_path.relative_to(root_dataset_path)
                if aggregate is True:

                    logger.debug(
                        f"Aggregate dataset "
                        f"{root_dataset_path / inter_dataset_path} "
                        f"to {root_dataset_path}[{inter_dataset_path}]")

                    extract_dataset_level_metadata(
                        root_dataset_path / inter_dataset_path,
                        metalad_arguments,
                        dict(
                            root_dataset_path=str(root_dataset_path),
                            root_dataset_id=root_dataset_id,
                            root_dataset_version=root_dataset_version,
                            inter_dataset_path=str(inter_dataset_path)
                        ))

                else:

                    logger.debug(
                        f"Extract dataset "
                        f"{root_dataset_path / inter_dataset_path} "
                        f"to {root_dataset_path / inter_dataset_path}[]")

                    extract_dataset_level_metadata(
                        root_dataset_path / inter_dataset_path,
                        metalad_arguments,
                        dict(
                            root_dataset_path=str(
                                root_dataset_path / inter_dataset_path)
                        ))

            current_dataset_path = current_path

        for entry in os.scandir(str(current_path)):
            if should_be_ignored(entry.name):
                continue
            extract_recursive(
                root_dataset_path,
                root_dataset_id,
                root_dataset_version,
                current_dataset_path,
                current_path / entry.name,
                metalad_arguments,
                dataset_recursive,
                aggregate)
    else:
        intra_dataset_path = current_path.relative_to(current_dataset_path)
        inter_dataset_path = current_dataset_path.relative_to(root_dataset_path)
        if aggregate is True:

            logger.debug(
                f"Aggregate file    {current_path} "
                f"to {intra_dataset_path} in "
                f"{root_dataset_path}[{inter_dataset_path}]")

            extract_file_level_metadata(
                current_dataset_path,
                intra_dataset_path,
                metalad_arguments,
                dict(
                    root_dataset_path=str(root_dataset_path),
                    root_dataset_id=root_dataset_id,
                    root_dataset_version=root_dataset_version,
                    inter_dataset_path=str(inter_dataset_path)
                ))
        else:

            logger.debug(
                f"Extract file    {current_path} "
                f"to {intra_dataset_path} in "
                f"{current_dataset_path}[]")

            extract_file_level_metadata(
                current_dataset_path,
                intra_dataset_path,
                metalad_arguments,
                dict(
                    root_dataset_path=str(current_dataset_path)
                ))


def extract(top_dir_path: Path, recursive: bool, aggregate: bool) -> None:

    dataset_id, dataset_version = is_dataset(top_dir_path)
    extract_recursive(
        root_dataset_path=top_dir_path,
        root_dataset_id=dataset_id,
        root_dataset_version=dataset_version,
        current_dataset_path=top_dir_path,
        current_path=top_dir_path,
        metalad_arguments=arguments.metalad_arguments,
        dataset_recursive=recursive,
        aggregate=aggregate)


def main() -> int:
    logging.basicConfig(level=log_level[arguments.log_level])

    if arguments.process_limit < 1:
        print(
            "Error: number of processes must be greater or equal to 1",
            file=sys.stderr)
        return 1

    if arguments.aggregate is True:
        if arguments.recursive is False:
            print(
                "Warning: 'aggregate' ignored, since 'recursive' is not set",
                file=sys.stderr)
            arguments.aggregate = False
        if arguments.save_metadata is None:
            print(
                "Warning: 'aggregate' ignored, since 'save_metadata' "
                "is not set",
                file=sys.stderr)
            arguments.aggregate = None

    if arguments.metadata_store:
        if arguments.save_metadata is False:
            print(
                "Warning: 'metadata-store' ignored, since 'save_metadata' "
                "is not set",
                file=sys.stderr)
            arguments.metadata_store = None

    extract(
        Path(arguments.dataset_path),
        arguments.recursive,
        arguments.aggregate)

    ensure_less_processes_than(1)
    return 0


if __name__ == "__main__":
    exit(main())
