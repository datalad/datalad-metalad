# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""
Run a dataset-level metadata extractor on a dataset
or run a file-level metadata extractor on a file
"""
import logging
import tempfile
import time
from os import curdir
from pathlib import Path
from typing import List, Optional, Tuple, Type, Union
from uuid import UUID

from dataclasses import dataclass

from datalad.config import ConfigManager
from datalad.distribution.dataset import Dataset
from datalad.interface.base import Interface
from datalad.interface.base import build_doc
from datalad.interface.common_opts import recursion_flag
from datalad.interface.utils import eval_results
from datalad.distribution.dataset import (
    datasetmethod,
    EnsureDataset,
    require_dataset,
)
from .extractors.base import (
    DataOutputCategory,
    DatasetMetadataExtractor,
    FileInfo,
    FileMetadataExtractor,
    MetadataExtractor,
    MetadataExtractorBase
)

from datalad.support.annexrepo import AnnexRepo
from datalad.support.gitrepo import GitRepo
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr
)
from datalad.support.param import Parameter

from dataladmetadatamodel.connector import Connector
from dataladmetadatamodel.datasettree import DatasetTree
from dataladmetadatamodel.filetree import FileTree
from dataladmetadatamodel.mapper.gitmapper.objectreference import flush_object_references
from dataladmetadatamodel.mapper.gitmapper.utils import lock_backend, unlock_backend
from dataladmetadatamodel.metadata import ExtractorConfiguration, Metadata
from dataladmetadatamodel.metadatarootrecord import MetadataRootRecord
from dataladmetadatamodel.uuidset import UUIDSet
from dataladmetadatamodel.versionlist import TreeVersionList, VersionList

from .extractors.base import ExtractorResult
from .metadata import get_top_level_metadata_objects


__docformat__ = 'restructuredtext'

default_mapper_family = "git"

lgr = logging.getLogger('datalad.metadata.extract')


@dataclass
class ExtractionParameter:
    realm: Union[AnnexRepo, GitRepo]
    source_dataset: Dataset
    source_dataset_id: UUID
    extractor_class: Union[type(MetadataExtractor), type(FileMetadataExtractor)]
    extractor_name: str
    dataset_tree_path: str
    file_tree_path: str
    root_primary_data_version: str
    source_primary_data_version: str
    agent_name: str
    agent_email: str


@build_doc
class Extract(Interface):
    """Run a metadata extractor on a dataset or file.

    This command distinguishes between dataset-level extraction and
    file-level extraction.

    If no "path" argument is given, the command
    assumes that a given extractor is a dataset-level extractor and
    executes it on the dataset that is given by the current working
    directory or by the "-d" argument.

    If a path is given, the command assumes that the given extractor is
    a file-level extractor and executes it on the file that is given as
    path parameter. If the file level extractor requests the content of
    a file that is not present, the command might "get" the file content
    to make it locally available.

    [NOT IMPLEMENTED YET] The extractor configuration can be
    parameterized with key-value pairs given as additional arguments.

    The results are written into the repository of the source dataset
    or into the repository of the dataset given by the "-i" parameter.
    If the same extractor is executed on the same element (dataset or
    file) with the same configuration, any existing results will be
    overwritten.

    Examples:

      Use the metalad_core_file-extractor to extract metadata from the
      file "subdir/data_file_1.txt". The dataset is given by the
      current working directory:

        $ datalad meta-extract metalad_core_file subdir/data_file_1.txt

      Use the metalad_core_file-extractor to extract metadata from the
      file "subdir/data_file_1.txt" in the dataset ds0001.

        $ datalad meta-extract -d ds0001 metalad_core_file subdir/data_file_1.txt

      Use the metalad_core_dataset-extractor to extract dataset-level
      metadata from the dataset given by the current working directory.

        $ datalad meta-extract metalad_core_dataset

      Use the metalad_core_dataset-extractor to extract dataset-level
      metadata from the dataset in /datasets/ds0001.

        $ datalad meta-extract -d /datasets/ds0001 metalad_core_dataset

      The command can also take legacy datalad-metalad extractors and
      will execute them in either "content" or "dataset" mode, depending
      on the presence of the "path"-parameter.
    """
    result_renderer = 'tailored'

    _params_ = dict(
        extractorname=Parameter(
            args=("extractorname",),
            metavar="EXTRACTOR_NAME",
            doc="Name of a metadata extractor to be executed."),
        path=Parameter(
            args=("path",),
            metavar="FILE",
            nargs="?",
            doc="""Path of a file or dataset to extract metadata
            from. If this argument is provided, we assume a file
            extractor is requested, if the path is not given, or
            if it identifies the root of a dataset, i.e. "", we
            assume a dataset level metadata extractor is
            specified.""",
            constraints=EnsureStr() | EnsureNone()),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc=""""Dataset to extract metadata from. If no dataset
            is given, the dataset is determined by the current work
            directory.""",
            constraints=EnsureDataset() | EnsureNone()),
        into=Parameter(
            args=("-i", "--into"),
            doc=""""Dataset to extract metadata into. This must be
            the dataset from which we extract metadata itself (the
            default) or a parent dataset of the dataset from
            which we extract metadata.""",
            constraints=EnsureDataset() | EnsureNone()),
        recursive=recursion_flag)

    @staticmethod
    @datasetmethod(name='meta_extract')
    @eval_results
    def __call__(
            extractorname: str,
            path: Optional[str] = None,
            dataset: Optional[str] = None,
            into: Optional[str] = None,
            recursive=False):

        # Get basic arguments
        source_dataset = require_dataset(
            dataset or curdir,
            purpose="extract metadata",
            check_installed=path is not None)
        source_primary_data_version = source_dataset.repo.get_hexsha()

        if into:
            into_ds = require_dataset(
                into,
                purpose="extract metadata",
                check_installed=True)
            realm = into_ds.repo
            root_primary_data_version = into_ds.repo.get_hexsha()
        else:
            realm = source_dataset.repo
            root_primary_data_version = source_primary_data_version

        extractor_class = get_extractor_class(extractorname)
        dataset_tree_path, file_tree_path = get_path_info(source_dataset, path, into)

        config_manager = ConfigManager()
        extraction_parameters = ExtractionParameter(
            realm,
            source_dataset,
            UUID(source_dataset.id),
            extractor_class,
            extractorname,
            dataset_tree_path,
            file_tree_path,
            root_primary_data_version,
            source_primary_data_version,
            config_manager.get("user.name"),
            config_manager.get("user.email"))

        # If a path is given, we assume file-level metadata extraction is
        # requested, and the extractor class is  a subclass of
        # FileMetadataExtractor. If oath is not given, we assume that
        # dataset-level extraction is requested and the extractor
        # class is a subclass of DatasetMetadataExtractor
        if path:
            yield from do_file_extraction(extraction_parameters)
        else:
            yield from do_dataset_extraction(extraction_parameters)

        return


def do_dataset_extraction(ep: ExtractionParameter):

    if not issubclass(ep.extractor_class, MetadataExtractorBase):

        lgr.info(
            "performing legacy dataset level metadata "
            "extraction for dataset at at %s",
            ep.source_dataset.path)

        yield from legacy_extract_dataset(ep)
        return

    lgr.info(
        "extracting dataset level metadata for dataset at %s",
        ep.source_dataset.path)

    assert issubclass(ep.extractor_class, DatasetMetadataExtractor)

    extractor = ep.extractor_class(
        ep.source_dataset,
        ep.source_primary_data_version)

    yield from perform_dataset_metadata_extraction(ep, extractor)


def do_file_extraction(ep: ExtractionParameter):

    if not issubclass(ep.extractor_class, MetadataExtractorBase):

        lgr.info(
            "performing legacy file level metadata "
            "extraction for file at %s/%s",
            ep.source_dataset.path,
            ep.file_tree_path)

        yield from legacy_extract_file(ep)
        return

    lgr.info(
        "performing file level extracting for file at %s/%s",
        ep.source_dataset.path,
        ep.file_tree_path)

    assert issubclass(ep.extractor_class, FileMetadataExtractor)
    file_info = get_file_info(ep.source_dataset, ep.file_tree_path)
    if file_info is None:
        raise FileNotFoundError(
            "file not found {}/{}".format(
                ep.source_dataset.path,
                ep.file_tree_path))

    extractor = ep.extractor_class(
        ep.source_dataset,
        ep.source_primary_data_version,
        file_info)

    ensure_content_availability(extractor, file_info)

    yield from perform_file_metadata_extraction(ep, extractor)


def perform_file_metadata_extraction(ep: ExtractionParameter,
                                     extractor: FileMetadataExtractor):

    output_category = extractor.get_data_output_category()
    if output_category == DataOutputCategory.IMMEDIATE:

        # Process immediate results
        result = extractor.extract(None)
        if result.extraction_success:
            add_file_metadata_source(
                ep,
                result,
                result.immediate_data)
        yield result.datalad_result_dict

    elif output_category == DataOutputCategory.FILE:

        # Process file-based results
        with tempfile.NamedTemporaryFile(mode="bw+") as temporary_file_info:
            result = extractor.extract(temporary_file_info)
            if result.extraction_success:
                add_file_metadata(
                    ep,
                    result,
                    temporary_file_info.name)
            yield result.datalad_result_dict

    elif output_category == DataOutputCategory.DIRECTORY:

        # Process directory results
        raise NotImplementedError

    lgr.info(
        f"added file metadata result to realm {repr(ep.realm)}, "
        f"dataset tree path {repr(ep.dataset_tree_path)}, "
        f"file tree path {repr(ep.file_tree_path)}")

    return


def perform_dataset_metadata_extraction(ep: ExtractionParameter,
                                        extractor: DatasetMetadataExtractor):

    output_category = extractor.get_data_output_category()
    if output_category == DataOutputCategory.IMMEDIATE:
        # Process inline results
        result = extractor.extract(None)
        if result.extraction_success:
            add_dataset_metadata_source(
                ep,
                result,
                result.immediate_data)
        yield result.datalad_result_dict

    elif output_category == DataOutputCategory.FILE:
        # Process file-based results
        with tempfile.NamedTemporaryFile(mode="bw+") as temporary_file_info:
            result = extractor.extract(temporary_file_info)
            if result.extraction_success:
                add_dataset_metadata(
                    ep,
                    result,
                    temporary_file_info.name)
            yield result.datalad_result_dict

    elif output_category == DataOutputCategory.DIRECTORY:
        # Process directory results
        raise NotImplementedError

    lgr.info(
        f"added dataset metadata result to realm {repr(ep.realm)}, "
        f"dataset tree path {repr(ep.dataset_tree_path)})")

    return


def get_extractor_class(extractor_name: str) -> Union[
                                            Type[DatasetMetadataExtractor],
                                            Type[FileMetadataExtractor]]:

    """ Get an extractor from its name """
    from pkg_resources import iter_entry_points  # delayed heavy import

    entry_points = list(iter_entry_points('datalad.metadata.extractors', extractor_name))

    if not entry_points:
        raise ValueError(
            "Requested metadata extractor '{}' not available".format(
                extractor_name))

    entry_point, ignored_entry_points = entry_points[-1], entry_points[:-1]
    lgr.debug(
        'Using metadata extractor %s from distribution %s',
        extractor_name,
        entry_point.dist.project_name)

    # Inform about overridden entry points
    for ignored_entry_point in ignored_entry_points:
        lgr.warning(
            'Metadata extractor %s from distribution %s overrides '
            'metadata extractor from distribution %s',
            extractor_name,
            entry_point.dist.name,
            ignored_entry_point.dist.project_name)

    return entry_point.load()


def get_file_info(dataset: Dataset, path: str) -> Optional[FileInfo]:
    """
    Get information about the file in the dataset or
    None, if the file is not part of the dataset.
    """
    if not path.startswith(dataset.path):
        path = dataset.path + "/" + path    # TODO: how are paths represented in datalad?

    path_status = (list(dataset.status(
        path,
        result_renderer='disabled')) or [None])[0]
    if path_status is None:
        return None

    # noinspection PyUnresolvedReferences
    return FileInfo(
        type=path_status["type"],
        git_sha_sum=path_status["gitshasum"],
        byte_size=path_status.get("bytesize", 0),
        state=path_status["state"],
        path=path_status["path"],            # TODO: use the dataset-tree path here?
        intra_dataset_path=path_status["path"][len(dataset.path) + 1:])


def get_path_info(dataset: Dataset,
                  path: Optional[str],
                  into_dataset: Optional[str] = None
                  ) -> Tuple[str, str]:
    """
    Determine the dataset tree path and the file tree path.

    If the path is absolute, we can determine the containing dataset
    and the metadatasets around it. If the path is not an element of
    a locally known dataset, we signal an error.

    If the pass is relative, we convert it to an absolute path
    by appending it to the dataset or current directory and perform
    the above check.
    """
    full_dataset_path = Path(dataset.path).resolve()
    if into_dataset is None:
        dataset_tree_path = ""
    else:
        full_into_dataset_path = Path(into_dataset).resolve()
        dataset_tree_path = str(full_dataset_path.relative_to(full_into_dataset_path))

    if path is None:
        return (
            ""
            if dataset_tree_path == "."
            else dataset_tree_path,
            "")

    given_file_path = Path(path)
    if given_file_path.is_absolute():
        full_file_path = given_file_path
    else:
        full_file_path = full_dataset_path / given_file_path

    file_tree_path = str(full_file_path.relative_to(full_dataset_path))

    return (
        ""
        if dataset_tree_path == "."
        else dataset_tree_path,
        file_tree_path)


def ensure_content_availability(extractor: FileMetadataExtractor,
                                file_info: FileInfo):

    if extractor.is_content_required():
        for result in extractor.dataset.get(path={file_info.path},
                                            get_data=True,
                                            return_type='generator',
                                            result_renderer='disabled'):
            if result.get("status", "") == "error":
                lgr.error(
                    "cannot make content of {} available in dataset {}".format(
                        file_info.path, extractor.dataset))
                return
        lgr.debug(
            "requested content {}:{} available".format(
                extractor.dataset.path, file_info.intra_dataset_path))


def get_top_nodes_and_mrr(ep: ExtractionParameter):
    tree_version_list, uuid_set = get_top_level_metadata_objects(default_mapper_family, ep.realm.path)
    if tree_version_list is None:
        tree_version_list = TreeVersionList(default_mapper_family, ep.realm.path)

    if uuid_set is None:
        uuid_set = UUIDSet(default_mapper_family, ep.realm.path)

    if ep.source_dataset_id in uuid_set.uuids():
        uuid_version_list = uuid_set.get_version_list(ep.source_dataset_id)
    else:
        uuid_version_list = VersionList(default_mapper_family, ep.realm.path)
        uuid_set.set_version_list(ep.source_dataset_id, uuid_version_list)

    # Get the dataset tree
    if ep.root_primary_data_version in tree_version_list.versions():
        time_stamp, dataset_tree = tree_version_list.get_dataset_tree(ep.root_primary_data_version)
    else:
        time_stamp = str(time.time())
        dataset_tree = DatasetTree(default_mapper_family, ep.realm.path)
        tree_version_list.set_dataset_tree(ep.root_primary_data_version, time_stamp, dataset_tree)

    if ep.dataset_tree_path not in dataset_tree:
        # Create a metadata root record-object and a dataset level metadata-object
        dataset_level_metadata = Metadata(default_mapper_family, ep.realm.path)
        file_tree = FileTree(default_mapper_family, ep.realm.path)
        mrr = MetadataRootRecord(
            default_mapper_family,
            ep.realm.path,
            ep.source_dataset_id,
            ep.source_primary_data_version,
            Connector.from_object(dataset_level_metadata),
            Connector.from_object(file_tree))
        dataset_tree.add_dataset(ep.dataset_tree_path, mrr)
    else:
        mrr = dataset_tree.get_metadata_root_record(ep.dataset_tree_path)

    uuid_version_list.set_versioned_element(
        ep.source_primary_data_version,
        str(time.time()),
        ep.dataset_tree_path,
        mrr)

    return tree_version_list, uuid_set, mrr


def add_file_metadata_source(ep: ExtractionParameter,
                             result: ExtractorResult,
                             metadata_source: dict):

    lock_backend(ep.realm.path)

    tree_version_list, uuid_set, mrr = get_top_nodes_and_mrr(ep)

    file_tree = mrr.get_file_tree()
    if file_tree is None:
        file_tree = FileTree(default_mapper_family, ep.realm.path)
        mrr.set_file_tree(file_tree)

    if ep.file_tree_path in file_tree:
        metadata = file_tree.get_metadata(ep.file_tree_path)
    else:
        metadata = Metadata(default_mapper_family, ep.realm.path)
        file_tree.add_metadata(ep.file_tree_path, metadata)

    metadata.add_extractor_run(
        time.time(),
        ep.extractor_name,
        ep.agent_name,
        ep.agent_email,
        ExtractorConfiguration(
            result.extractor_version,
            result.extraction_parameter),
        metadata_source)

    tree_version_list.save()
    uuid_set.save()
    flush_object_references(ep.realm.path)

    unlock_backend(ep.realm.path)


def add_dataset_metadata_source(ep: ExtractionParameter,
                                result: ExtractorResult,
                                metadata_source: dict):

    lock_backend(ep.realm.path)

    tree_version_list, uuid_set, mrr = get_top_nodes_and_mrr(ep)

    dataset_level_metadata = mrr.get_dataset_level_metadata()
    if dataset_level_metadata is None:
        dataset_level_metadata = Metadata(default_mapper_family, ep.realm.path)
        mrr.set_dataset_level_metadata(dataset_level_metadata)

    dataset_level_metadata.add_extractor_run(
        time.time(),
        ep.extractor_name,
        ep.agent_name,
        ep.agent_email,
        ExtractorConfiguration(
            result.extractor_version,
            result.extraction_parameter),
        metadata_source)

    tree_version_list.save()
    uuid_set.save()
    flush_object_references(ep.realm.path)

    unlock_backend(ep.realm.path)


def add_file_metadata(ep: ExtractionParameter,
                      result: ExtractorResult,
                      metadata_file_path: str):

    # copy the temporary file content into the git repo
    git_object_hash = copy_file_to_git(metadata_file_path, ep.realm)

    add_file_metadata_source(ep, result, {
            "type": "git-object",
            "location": git_object_hash
        })


def add_dataset_metadata(ep: ExtractionParameter,
                         result: ExtractorResult,
                         metadata_file_path: str):

    # copy the temporary file content into the git repo
    git_object_hash = copy_file_to_git(metadata_file_path, ep.realm)

    add_dataset_metadata_source(ep, result, {
            "type": "git-object",
            "location": git_object_hash
        })


def copy_file_to_git(file_path: str, realm: Union[AnnexRepo, GitRepo]):
    arguments = [f"--git-dir={realm.path + '/.git'}", "hash-object", "-w", "--", file_path]
    return realm.call_git_oneline(arguments)


def ensure_legacy_content_availability(ep: ExtractionParameter,
                                       extractor: MetadataExtractor,
                                       operation: str,
                                       status: List[dict]):

    try:
        for required_element in extractor.get_required_content(ep.source_dataset, operation, status):
            for result in ep.source_dataset.get(path={required_element.path},
                                                get_data=True,
                                                return_type='generator',
                                                result_renderer='disabled'):
                if result.get("status", "") == "error":
                    lgr.error(
                        "cannot make content of {} available in dataset {}".format(
                            required_element.path, ep.source_dataset))
                    return
            lgr.debug(
                "requested content {}:{} available".format(
                    ep.source_dataset.path, required_element.path))
    except AttributeError:
        pass


def legacy_extract_dataset(ep: ExtractionParameter):

    extractor = ep.extractor_class()
    status = [{
        "type": "dataset",
        "path": ep.realm.path + "/" + ep.dataset_tree_path,
        "state": "clean"
    }]

    ensure_legacy_content_availability(ep, extractor, "dataset", status)

    for result in extractor(ep.source_dataset, ep.source_dataset.repo.get_hexsha(), "dataset", status):
        if result["status"] == "ok":
            extractor_result = ExtractorResult(
                "0.1",
                extractor.get_state(ep.source_dataset),
                True,
                result,
                result["metadata"])

            add_dataset_metadata_source(
                ep,
                extractor_result,
                extractor_result.immediate_data)
        yield result


def legacy_extract_file(ep: ExtractionParameter):

    extractor = ep.extractor_class()
    status = [{
        "type": "file",
        "path": ep.realm.path + "/" + ep.dataset_tree_path + "/" + ep.file_tree_path,
        "state": "clean"
    }]

    ensure_legacy_content_availability(ep, extractor, "content", status)

    for result in extractor(ep.source_dataset, ep.source_dataset.repo.get_hexsha(), "content", status):
        if result["status"] == "ok":
            extractor_result = ExtractorResult(
                "0.1",
                extractor.get_state(ep.source_dataset),
                True,
                result,
                result["metadata"])

            add_file_metadata_source(
                ep,
                extractor_result,
                extractor_result.immediate_data)
        yield result
