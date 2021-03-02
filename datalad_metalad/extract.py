# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Run one or more metadata extractors on a dataset or file(s)"""
import logging
import tempfile
import os.path as op
import subprocess
import time
from os import curdir
from pathlib import PosixPath
from six import (
    iteritems,
    text_type,
)
from typing import Any, Dict, Optional, Tuple, Type, Union
from uuid import UUID

from datalad import cfg
from datalad.distribution.dataset import Dataset
from datalad.interface.base import Interface
from datalad.interface.base import build_doc
from datalad.interface.common_opts import recursion_flag
from datalad.interface.results import (
    get_status_dict,
    success_status_map,
)
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
    FileMetadataExtractor
)

from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr,
    EnsureChoice,
    EnsureBool,
)
from . import (
    get_refcommit,
    exclude_from_metadata,
    get_metadata_type,
    collect_jsonld_metadata,
    format_jsonld_metadata,
)
from datalad.utils import (
    assure_list,
    Path,
    PurePosixPath,
)
from datalad.dochelpers import exc_str
from datalad.log import log_progress
from datalad.ui import ui
import datalad.support.ansi_colors as ac
from simplejson import dumps as jsondumps

# API commands needed
from datalad.core.local import status as _status

from dataladmetadatamodel.connector import Connector
from dataladmetadatamodel.datasettree import DatasetTree
from dataladmetadatamodel.filetree import FileTree
from dataladmetadatamodel.mapper import (
    get_tree_version_list_location,
    get_uuid_set_location
)
from dataladmetadatamodel.mapper.gitmapper.objectreference import flush_object_references
from dataladmetadatamodel.metadata import ExtractorConfiguration, Metadata
from dataladmetadatamodel.metadatarootrecord import MetadataRootRecord
from dataladmetadatamodel.uuidset import UUIDSet
from dataladmetadatamodel.versionlist import TreeVersionList, VersionList

from .extractors.base import ExtractorResult
from .metadata import get_top_level_metadata_objects


__docformat__ = 'restructuredtext'


default_mapper_family = "git"

lgr = logging.getLogger('datalad.metadata.extract')


@build_doc
class Extract(Interface):
    """Run one or more metadata extractors on a dataset or file.

    This command does not modify a dataset, but may initiate required data
    transfers to perform metadata extraction that requires local file content
    availability. This command does not support recursion into subdataset.

    The result(s) are structured like the metadata DataLad would extract
    during metadata aggregation (in fact, this command is employed during
    aggregation). There is one result per dataset/file.

    Examples:

      Extract metadata with two extractors from a dataset in the current
      directory and also from all its files::

        $ datalad meta-extract -d . --source xmp --source metalad_core

      Extract XMP metadata from a single PDF that is not part of any dataset::

        $ datalad meta-extract --source xmp Downloads/freshfromtheweb.pdf


    Customization of extraction:

    The following configuration settings can be used to customize extractor
    behavior

    ``datalad.metadata.extract-from-<extractorname> = {all|dataset|content}``
       which type of information an enabled extractor will be operating on
       (see --process-type argument for details)

    ``datalad.metadata.exclude-path = <path>``
      ignore all content underneath the given path for metadata extraction,
      must be relative to the root of the dataset and in POSIX convention,
      and can be given multiple times
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
            the dataset from which we extract metadata itself -which
            is the default- or a parent dataset of the dataset from
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

        ds = require_dataset(
            dataset or curdir,
            purpose="extract metadata",
            check_installed=path is not None)

        if into:
            into_ds = require_dataset(
                into,
                purpose="extract metadata",
                check_installed=True)

        source_primary_data_version = ds.repo.get_hexsha()
        extractor_class = get_extractor_class(extractorname)

        dataset_tree_path, file_tree_path = get_path_info(ds, path, into)
        if into:
            realm = into_ds.path
            root_primary_data_version = into_ds.repo.get_hexsha()
        else:
            realm = ds.path
            root_primary_data_version = source_primary_data_version

        if not path:

            # Try to perform dataset level metadata extraction
            lgr.info("extracting dataset level metadata for dataset at %s", ds.path)

            assert isinstance(extractor_class, type(DatasetMetadataExtractor))
            extractor = extractor_class(ds, source_primary_data_version)

        else:

            # Try to perform file level metadata extraction
            lgr.info("extracting file level metadata for file at %s:%s", ds.path, path)

            assert isinstance(extractor_class, type(FileMetadataExtractor))
            file_info = get_file_info(ds, path)
            if file_info is None:
                raise FileNotFoundError(
                    "{} not found in dataset {}".format(
                        path, dataset or curdir))

            extractor = extractor_class(ds, source_primary_data_version, file_info)
            ensure_content_availability(extractor, file_info)

        output_category = extractor.get_data_output_category()
        if output_category == DataOutputCategory.IMMEDIATE:

            # Process inline results
            result = extractor.extract(None)
            if result.extraction_success:
                add_immediate_metadata(
                    extractorname,
                    realm,
                    root_primary_data_version,
                    UUID(ds.id),
                    source_primary_data_version,
                    dataset_tree_path,
                    file_tree_path,
                    result)

            yield result.datalad_result_dict

        elif output_category == DataOutputCategory.FILE:

            with tempfile.NamedTemporaryFile(mode="bw+") as temporary_file_info:

                result = extractor.extract(temporary_file_info)
                if result.extraction_success:
                    add_file_content_to_metadata(
                        extractorname,
                        realm,
                        root_primary_data_version,
                        UUID(ds.id),
                        source_primary_data_version,
                        dataset_tree_path,
                        file_tree_path,
                        result,
                        temporary_file_info.name)

                yield result.datalad_result_dict

        elif output_category == DataOutputCategory.DIRECTORY:

            # Process directory results
            raise NotImplementedError

        lgr.info(
            f"adding metadata result to realm {repr(realm)}, "
            f"dataset tree path {repr(dataset_tree_path)}, "
            f"file tree path {repr(file_tree_path)}")

        return


def get_extractor_class(extractor_name: str) -> Union[
                                            Type[DatasetMetadataExtractor],
                                            Type[FileMetadataExtractor]]:

    """ Get an extractor from its name """
    from pkg_resources import iter_entry_points  # delayed heavy import

    entry_points = list(
        iter_entry_points('datalad.metadata.extractors', extractor_name))

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

    :param dataset:
    :param path:
    :return:
    """
    if not path.startswith(dataset.path):
        path = dataset.path + "/" + path    # TODO: how are paths represented in datalad?

    path_status = (list(dataset.status(path)) or [None])[0]
    if path_status is None:
        return None

    return FileInfo(
        path_status["type"],
        path_status["gitshasum"],
        path_status.get("bytesize", 0),
        path_status["state"],
        path_status["path"],            # TODO: use the dataset-tree path here?
        path_status["path"][len(dataset.path) + 1:])


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
    dataset_path = PosixPath(dataset.path)
    if path is None:
        return str(dataset_path), ""

    given_file_path = PosixPath(path)
    if given_file_path.is_absolute():
        full_path = given_file_path
    else:
        full_path = (dataset_path / given_file_path).resolve()

    file_tree_path = str(full_path.relative_to(dataset_path))

    if into_dataset is None:
        dataset_tree_path = ""
    else:
        into_dataset_path = PosixPath(into_dataset)
        dataset_tree_path = str(dataset_path.relative_to(into_dataset_path))

    return dataset_tree_path, file_tree_path


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


def add_immediate_metadata(extractor_name: str,
                           realm: str,
                           root_primary_data_version: str,
                           dataset_id: UUID,
                           dataset_primary_data_version: str,
                           dataset_tree_path: str,
                           file_tree_path: str,
                           result: ExtractorResult):

    tree_version_list, uuid_set = get_top_level_metadata_objects(default_mapper_family, realm)
    if tree_version_list is None:
        tree_version_list = TreeVersionList(default_mapper_family, realm)

    # Get the dataset tree
    if root_primary_data_version in tree_version_list.versions():
        time_stamp, dataset_tree = tree_version_list.get_dataset_tree(root_primary_data_version)
    else:
        time_stamp = str(time.time())
        dataset_tree = DatasetTree(default_mapper_family, realm)
        tree_version_list.set_dataset_tree(root_primary_data_version, time_stamp, dataset_tree)

    if dataset_tree_path not in dataset_tree:
        # Create a metadata root record-object and a file tree-object
        file_tree = FileTree(default_mapper_family, realm)
        mrr = MetadataRootRecord(
            default_mapper_family,
            realm,
            dataset_id,
            dataset_primary_data_version,
            Connector.from_object(None),
            Connector.from_object(file_tree))
        dataset_tree.add_dataset(dataset_tree_path, mrr)
    else:
        mrr = dataset_tree.get_metadata_root_record(dataset_tree_path)

    file_tree = mrr.get_file_tree()
    if file_tree_path in file_tree:
        metadata = file_tree.get_metadata(file_tree_path)
    else:
        metadata = Metadata(default_mapper_family, realm)
        file_tree.add_metadata(file_tree_path, metadata)

    metadata.add_extractor_run(
        time.time(),
        extractor_name,
        "Christian Mönch",
        "c.moench@fz-juelich.de",
        ExtractorConfiguration(
            result.extractor_version,
            result.extraction_parameter),
        result.immediate_data)

    tree_version_list.save()

    if uuid_set is None:
        uuid_set = UUIDSet(default_mapper_family, realm)

    if dataset_id in uuid_set.uuids():
        version_list = uuid_set.get_version_list(dataset_id)
    else:
        version_list = VersionList(default_mapper_family, realm)
        uuid_set.set_version_list(dataset_id, version_list)

    version_list.set_versioned_element(
        dataset_primary_data_version,
        str(time.time()),
        dataset_tree_path,
        mrr)

    uuid_set.save()

    flush_object_references(realm)


def add_file_content_to_metadata(extractor_name: str,
                                 realm: str,
                                 root_primary_data_version: str,
                                 dataset_id: UUID,
                                 dataset_primary_data_version: str,
                                 dataset_tree_path: str,
                                 file_tree_path: str,
                                 result: ExtractorResult,
                                 metadata_file_path: str):

    tree_version_list, uuid_set = get_top_level_metadata_objects(default_mapper_family, realm)
    if tree_version_list is None:
        tree_version_list = TreeVersionList(default_mapper_family, realm)

    # Get the dataset tree
    if root_primary_data_version in tree_version_list.versions():
        time_stamp, dataset_tree = tree_version_list.get_dataset_tree(root_primary_data_version)
    else:
        time_stamp = str(time.time())
        dataset_tree = DatasetTree(default_mapper_family, realm)
        tree_version_list.set_dataset_tree(root_primary_data_version, time_stamp, dataset_tree)

    mrr = dataset_tree.get_metadata_root_record(dataset_tree_path)
    if mrr is None:
        # Create a metadata root record-object and a file tree-object
        file_tree = FileTree(default_mapper_family, realm)
        mrr = MetadataRootRecord(
            default_mapper_family,
            realm,
            dataset_id,
            dataset_primary_data_version,
            Connector.from_object(None),
            Connector.from_object(file_tree))
        dataset_tree.add_dataset(dataset_tree_path, mrr)

    else:
        file_tree = mrr.get_file_tree()

    if file_tree_path in file_tree:
        metadata = file_tree.get_metadata(file_tree_path)
    else:
        metadata = Metadata(default_mapper_family, realm)
        file_tree.add_metadata(file_tree_path, metadata)

    # copy the temporary file content into the git repo
    git_object_hash = copy_file_to_git(metadata_file_path, realm)

    metadata.add_extractor_run(
        time.time(),
        extractor_name,
        "Christian Mönch",
        "c.moench@fz-juelich.de",
        ExtractorConfiguration(
            result.extractor_version,
            result.extraction_parameter),
        {
            "type": "git-object",
            "location": git_object_hash
        })

    tree_version_list.save()

    if uuid_set is None:
        uuid_set = UUIDSet(default_mapper_family, realm)

    if dataset_id in uuid_set.uuids():
        version_list = uuid_set.get_version_list(dataset_id)
    else:
        version_list = VersionList(default_mapper_family, realm)
        uuid_set.set_version_list(dataset_id, version_list)

    version_list.set_versioned_element(
        dataset_primary_data_version,
        str(time.time()),
        dataset_tree_path,
        mrr)

    uuid_set.save()

    flush_object_references(realm)


def copy_file_to_git(file_path: str, realm: str):
    arguments = ["git", f"--git-dir={realm + '/.git'}", "hash-object", "-w", "--", file_path]
    result = subprocess.run(arguments, stdout=subprocess.PIPE)
    if result.returncode != 0:
        raise ValueError(f"execution of `{' '.join(arguments)}´ failed with {result.returncode}")
    return result.stdout.decode().strip()


x = """
class XXASDAS:

    def xxx(self):
        if recursive:
            # when going recursive, we also want to capture all pre-aggregated
            # metadata underneath the discovered datasets
            # this trick looks for dataset records that have no query path
            # assigned already (which will be used to match aggregate records)
            # and assign them their own path). This will match all stored
            # records and captures the aggregate --recursive case without
            # a dedicated `path` argument.
            extract_from_ds = {
                k:
                v
                if len([i for i in v if not isinstance(i, Dataset)])
                else v.union([k.pathobj])
                for k, v in iteritems(extract_from_ds)
            }

        # keep local, who knows what some extractors might pull in
        from pkg_resources import iter_entry_points  # delayed heavy import
        extractors = {}
        for ep in iter_entry_points('datalad.metadata.extractors'):
            if ep.name not in sources:
                # not needed here
                continue
            rec = dict(entrypoint=ep)
            if ep.name in extractors:  # pragma: no cover
                # potential conflict
                if extractors[ep.name]['entrypoint'].dist.project_name == 'datalad':
                    # this is OK, just state it is happening
                    lgr.debug(
                        'Extractor %s overrides datalad-core variant', ep)
                    extractors[ep.name] = rec
                elif ep.dist.project_name == 'datalad':
                    # also OK
                    lgr.debug(
                        'Prefer extractor %s over datalad-core variant', ep)
                else:
                    msg = (
                        'At least two DataLad extensions provide metadata '
                        'extractor %s: %s vs. %s',
                        ep.name,
                        ep.dist,
                        extractors[ep.name].dist)
                    if ep.name in sources:
                        # this extractor is required -> blow hard
                        raise RuntimeError(msg[0] % msg[1:])
                    else:
                        # still moan
                        lgr.warn(msg)
                    # ignore the newcomer, is listed second in sys.path
            else:
                # this fresh and unique
                extractors[ep.name] = rec
        for msrc in sources:
            if msrc not in extractors:
                # we said that we want to fail, rather then just moan about
                # less metadata
                raise ValueError(
                    "Enabled metadata extractor '{}' not available".format(msrc))
            # load extractor implementation
            rec = extractors[msrc]
            rec['process_type'] = process_type \
                if process_type and not process_type == 'extractors' \
                else ds.config.obtain(
                    'datalad.metadata.extract-from-{}'.format(
                        msrc.replace('_', '-')),
                    default='all')
            # load the extractor class, no instantiation yet
            try:
                rec['class'] = rec['entrypoint'].load()
            except Exception as e:  # pragma: no cover
                msg = ('Failed %s metadata extraction from %s: %s',
                       msrc, ds, exc_str(e))
                log_progress(lgr.error, 'metadataextractors', *msg)
                raise ValueError(msg[0] % msg[1:])

        res_props = dict(
            action='meta_extract',
            logger=lgr,
        )

        # build report on extractors and their state info
        if process_type == 'extractors':
            for ename, eprops in iteritems(extractors):
                state = {}
                # do not trip over old extractors
                if hasattr(eprops['class'], 'get_state'):
                    state.update(eprops['class']().get_state(ds))

                yield dict(
                    action='meta_extract',
                    path=ds.path,
                    status='ok',
                    logger=lgr,
                    extractor=ename,
                    state=dict(
                        state,
                        process_type=eprops['process_type'],
                    )
                )
            return



        return

        # build a representation of the dataset's content (incl subds
        # records)
        # go through a high-level command (not just the repo methods) to
        # get all the checks and sanitization of input arguments
        # this call is relatively expensive, but already anticipates
        # demand for information by our core extractors that always run
        # unconditionally, hence no real slowdown here
        # TODO this could be a dict, but MIH cannot think of an access
        # pattern that does not involve iteration over all items
        status = []
        exclude_paths = [
            ds.pathobj / PurePosixPath(e)
            for e in (
                list(exclude_from_metadata) + assure_list(
                    ds.config.get('datalad.metadata.exclude-path', [])
                )
            )
        ]
        if ds.is_installed():
            # we can make use of status
            res_props.update(refds=ds.path)

            for r in ds.status(
                    # let status sort out all path arg handling
                    # but this will likely make it impossible to use this
                    # command to just process an individual file independent
                    # of a dataset
                    path=path,
                    # it is safe to ask for annex info even when a dataset is
                    # plain Git
                    # NOTE changing to 'annex=availability' has substantial
                    # performance costs, as it involved resolving each annex
                    # symlink on the file-system, which can be really slow
                    # depending on the FS and the number of annexed files
                    annex='basic',
                    # TODO we never want to aggregate metadata from untracked
                    # content, but we might just want to see what we can get
                    # from a file
                    untracked='no',
                    # this command cannot and will not work recursively
                    recursive=False,
                    result_renderer='disabled'):
                # path reports are always absolute and anchored on the dataset
                # (no repo) path
                p = Path(r['path'])
                if p in exclude_paths or \
                        any(e in p.parents for e in exclude_paths):
                    # this needs to be ignore for any further processing
                    continue
                # strip useless context information
                status.append(
                    {k: v for k, v in iteritems(r)
                     if (k not in ('refds', 'parentds', 'action', 'status')
                         and not k.startswith('prev_'))})

            # determine the commit that we are describing
            refcommit = get_refcommit(ds)
            if refcommit is None or not len(status):
                # this seems extreme, but without a single commit there is
                # nothing we can have, or describe -> blow
                yield dict(
                    res_props,
                    status='error',
                    message=
                    'No metadata-relevant repository content found. '
                    'Cannot determine reference commit for metadata ID',
                    type='dataset',
                    path=ds.path,
                )
                return
            # stamp every result
            res_props['refcommit'] = refcommit
        else:
            # no dataset at hand, take path arg at face value and hope
            # for the best
            # TODO we have to resolve the given path to make it match what
            # status is giving (abspath with ds (not repo) anchor)
            status = [dict(path=p, type='file') for p in assure_list(path)]
            # just for compatibility, mandatory argument list below
            refcommit = None

        if ds.is_installed():
            # check availability requirements and obtain data as needed
            needed_paths = set()
            for rec in extractors.values():
                if hasattr(rec['class'], 'get_required_content'):
                    needed_paths.update(
                        # new extractors do not need any instantiation args
                        s['path'] for s in rec['class']().get_required_content(
                            ds,
                            rec['process_type'],
                            status
                        )
                    )
            if needed_paths:
                for r in ds.get(
                        path=needed_paths,
                        return_type='generator',
                        result_renderer='disabled'):
                    if success_status_map.get(
                            r['status'],
                            False
                    ) != 'success':  # pragma: no cover
                        # online complain when something goes wrong
                        yield r

        contexts = {}
        nodes_by_context = {}
        try:
            for res in _proc(
                    ds,
                    refcommit,
                    sources,
                    status,
                    extractors,
                    process_type):
                if format == 'native':
                    # that is what we pass around internally
                    res.update(**res_props)
                    yield res
                elif format == 'jsonld':
                    collect_jsonld_metadata(
                        ds.pathobj, res, nodes_by_context, contexts)
        finally:
            # extractors can come from any source with no guarantee for
            # proper implementation. Let's make sure that we bring the
            # dataset back into a sane state (e.g. no batch processes
            # hanging around). We should do this here, as it is not
            # clear whether extraction results will be saved to the
            # dataset(which would have a similar sanitization effect)
            if ds.repo:
                ds.repo.precommit()
        if format == 'jsonld':
            yield dict(
                status='ok',
                type='dataset',
                path=ds.path,
                metadata=format_jsonld_metadata(nodes_by_context),
                **res_props)

    @staticmethod
    def find_extract_ds(path: str):
        if path:
            extract_from_ds, errors = sort_paths_by_datasets(
                ds, dataset, assure_list(path))
            for e in errors:  # pragma: no cover
                e.update(
                    logger=lgr,
                    refds=ds.path,
                )
                yield e
        else:
            extract_from_ds = OrderedDict({ds.pathobj: []})

        # convert the values into sets to ease processing below
        return {
            Dataset(k): set(assure_list(v))
            for k, v in iteritems(extract_from_ds)
        }


    @staticmethod
    def custom_result_renderer(res, **kwargs):
        if res['status'] != 'ok' or \
                not res.get('action', None) == 'meta_extract':
            # logging complained about this already
            return
        if 'state' in res and 'extractor' in res:
            # extractor report, special treatment
            ui.message('{name}({state})'.format(
                name=ac.color_word(res['extractor'], ac.BOLD),
                state=','.join('{}{}{}{}'.format(
                    # boolean states get a + or - prefix
                    '+' if v is True else '-' if v is False else '',
                    k,
                    '=' if not isinstance(v, bool) else '',
                    v if not isinstance(v, bool) else '')
                    for k, v in iteritems(res['state'])
                    # this is an extractor property, and mostly serves
                    # internal purposes
                    if k not in ('unique_exclude',)),
            ))
            return
        if kwargs.get('format', None) == 'jsonld':
            # special case of a JSON-LD report request
            # all reports are consolidated into a single
            # graph, dumps just that (no pretty printing, can
            # be done outside)
            ui.message(jsondumps(
                res['metadata'],
                # support utf-8 output
                ensure_ascii=False,
                # this cannot happen, spare the checks
                check_circular=False,
                # this will cause the output to not necessarily be
                # JSON compliant, but at least contain all info that went
                # in, and be usable for javascript consumers
                allow_nan=True,
            ))
            return
        # list the path, available metadata keys, and tags
        path = op.relpath(
            res['path'],
            res['refds']) if res.get('refds', None) else res['path']
        meta = res.get('metadata', {})
        ui.message('{path}{type}:{spacer}{meta}{tags}'.format(
            path=ac.color_word(path, ac.BOLD),
            type=' ({})'.format(
                ac.color_word(res['type'], ac.MAGENTA))
            if 'type' in res else '',
            spacer=' ' if len([m for m in meta if m != 'tag']) else '',
            meta=','.join(k for k in sorted(meta.keys())
                          if k not in ('tag', '@context', '@id'))
                 if meta else ' -' if 'metadata' in res else
                 ' {}'.format(
                     ','.join(e for e in res['extractors']
                              if e not in ('datalad_core', 'metalad_core', 'metalad_annex'))
                 ) if 'extractors' in res else '',
            tags='' if 'tag' not in meta else ' [{}]'.format(
                 ','.join(assure_list(meta['tag'])))))


def _create_metadata_object(
        mapper_family: str,
        realm: str,
        intra_dataset_path: str,
        extractor_name: str,
        version: str,
        metadata_result: str) -> Metadata:

    metadata = Metadata(mapper_family, realm)
    metadata.add_extractor_run(
        time.time(),
        extractor_name,
        "chm",
        "c.moench@fz-juelich.de",
        ExtractorConfiguration(version, {"parameter1": "value1"}),
        {"inline_metadata": metadata_result}
    )
    return metadata


def _proc(ds, refcommit, sources, status, extractors, process_type):
    dsmeta = dict()
    contentmeta = {}

    root_dir = ds.path
    md_dataset_tree = DatasetTree("git", root_dir)
    md_file_tree = FileTree("git", root_dir)

    print(f"_proc({ds}, {refcommit}, {sources}, {status}, {extractors}, {process_type}")
    log_progress(
        lgr.info,
        'metadataextractors',
        'Start metadata extraction from %s', ds,
        total=len(sources),
        label='Metadata extraction',
        unit=' extractors',
    )
    for msrc in sources:
        msrc_key = msrc
        extractor = extractors[msrc]
        log_progress(
            lgr.info,
            'metadataextractors',
            'Engage %s metadata extractor', msrc_key,
            update=1,
            increment=True)

        # actually pull the metadata records out of the extractor
        for res in _run_extractor(
                extractor['class'],
                msrc,
                ds,
                refcommit,
                status,
                extractor['process_type']):

            print(f"RESULT:{ds}, {msrc}, {res['type']}: {res}")
            if res['type'] == 'file':
                import json

                intra_dataset_path = res['path']
                if intra_dataset_path.startswith(ds.path):
                    intra_dataset_path = intra_dataset_path[len(ds.path) + 1:]

                md_file_tree.add_metadata(
                    intra_dataset_path,
                    _create_metadata_object(
                        "git",
                        ds.path,
                        intra_dataset_path,
                        msrc,
                        "UNKNOWN VERSION",
                        res))

            elif res['type'] == 'dataset':
                import json

                intra_dataset_path = ds.path
                print(f"DATASET PATH: {intra_dataset_path}")
                #md_ds_tree.add_metadata_source(
                #    path,
                #    msrc,
                #    create_immediate_source_from_text(json.dumps(res))
                #)

            # always have a path, use any absolute path coming in,
            # make any relative path absolute using the dataset anchor,
            # use the dataset path if nothing is coming in (better then
            # no path at all)
            # for now normalize the reported path to be a plain string
            # until DataLad as a whole can deal with pathlib objects
            if 'path' in res:
                res['path'] = text_type(Path(res['path']))
            res.update(
                path=ds.path
                if 'path' not in res else res['path']
                if op.isabs(res['path']) else op.join(ds.path, res['path'])
            )

            # the following two conditionals are untested, as a test would
            # require a metadata extractor to yield broken metadata, and in
            # order to have such one, we need a mechanism to have the test
            # inject one on the fly MIH thinks that the code neeeded to do that
            # is more chances to be broken then the code it would test
            if success_status_map.get(res['status'], False) != 'success':  # pragma: no cover
                yield res
                # no further processing of broken stuff
                continue
            else:  # pragma: no cover
                # if the extractor was happy check the result
                if not _ok_metadata(res, msrc, ds, None):
                    res.update(
                        # this will prevent further processing a few lines down
                        status='error',
                        # TODO have _ok_metadata report the real error
                        message=('Invalid metadata (%s)', msrc),
                    )
                    yield res
                    continue

            # we do not want to report info that there was no metadata
            if not res['metadata']:  # pragma: no cover
                lgr.debug(
                    'Skip %s %s metadata in record of %s: '
                    'extractor reported nothing',
                    msrc_key, res.get('type', ''), res['path'])
                continue

            if res['type'] == 'dataset':
                # TODO warn if two dataset records are generated by the same
                # extractor
                dsmeta[msrc_key] = res['metadata']
            else:
                # this is file metadata, _ok_metadata() checks unknown types
                # assign only ask each metadata extractor once, hence no
                # conflict possible
                loc_dict = contentmeta.get(res['path'], {})
                loc_dict[msrc_key] = res['metadata']
                contentmeta[res['path']] = loc_dict

    log_progress(
        lgr.info,
        'metadataextractors',
        'Finished metadata extraction from %s', ds,
    )

    print(f"md_file_tree for dataset {ds.path}")
    for file_path, metadata_connector in md_file_tree.get_paths_recursive():
        print(f"{ds.path}:{file_path}:\t\t{metadata_connector.object}")

    # top-level code relies on the fact that any dataset metadata
    # is yielded before content metadata
    if process_type in (None, 'all', 'dataset') and \
            dsmeta and ds is not None and ds.is_installed():
        yield get_status_dict(
            ds=ds,
            metadata=dsmeta,
            # any errors will have been reported before
            status='ok',
        )

    for p in contentmeta:
        res = get_status_dict(
            # TODO avoid is_installed() call
            path=op.join(ds.path, p) if ds.is_installed() else p,
            metadata=contentmeta[p],
            type='file',
            # any errors will have been reported before
            status='ok',
        )
        # TODO avoid is_installed() call, check if such info is
        # useful and accurate at all
        if ds.is_installed():
            res['parentds'] = ds.path
        yield res

def _run_extractor(extractor_cls, name, ds, refcommit, status, process_type):
    # Helper to control extractor using the right API
    #
    # Central switch to deal with alternative/future APIs is inside
    try:
        # detect supported API and interface as needed
        if issubclass(extractor_cls, MetadataExtractor):
            # new-style, command-like extractors
            extractor = extractor_cls()
            for r in extractor(
                    dataset=ds,
                    refcommit=refcommit,
                    status=status,
                    process_type=process_type):
                yield r
        elif hasattr(extractor_cls, 'get_metadata'):  # pragma: no cover
            # old-style, keep around for a while, but don't sweat over it much
            for res in _yield_res_from_pre2019_extractor(
                    ds,
                    name,
                    extractor_cls,
                    process_type,
                    # old extractors only take a list of relative paths
                    # and cannot benefit from outside knowledge
                    # TODO avoid is_installed() call
                    [text_type(Path(p['path']).relative_to(ds.pathobj))
                     if ds.is_installed()
                     else p['path']
                     for p in status]):
                yield res
        else:  # pragma: no cover
            raise RuntimeError(
                '{} does not have a recognised extractor API'.format(
                    extractor_cls))
    except Exception as e:  # pragma: no cover
        if cfg.get('datalad.runtime.raiseonerror'):
            log_progress(
                lgr.error,
                'metadataextractors',
                'Failed %s metadata extraction from %s', name, ds,
            )
            raise
        yield get_status_dict(
            ds=ds,
            # any errors will have been reported before
            status='error',
            message=('Failed to get %s metadata (%s): %s',
                     ds, name, exc_str(e)),
        )

def _yield_res_from_pre2019_extractor(
        ds, name, extractor_cls, process_type, paths):  # pragma: no cover
    # This implements dealing with our first extractor class concept

    want_dataset_meta = process_type in ('all', 'dataset') \
        if process_type else ds.config.obtain(
            'datalad.metadata.extract-dataset-{}'.format(
                name.replace('_', '-')),
            default=True,
            valtype=EnsureBool())
    want_content_meta = process_type in ('all', 'content') \
        if process_type else ds.config.obtain(
            'datalad.metadata.extract-content-{}'.format(
                name.replace('_', '-')),
            default=True,
            valtype=EnsureBool())

    if not (want_dataset_meta or want_content_meta):  # pragma: no cover
        log_progress(
            lgr.info,
            'metadataextractors',
            'Skipping %s metadata extraction from %s, '
            'disabled by configuration',
            name, ds,
        )
        return

    try:
        extractor = extractor_cls(ds, paths)
    except Exception as e:  # pragma: no cover
        log_progress(
            lgr.error,
            'metadataextractors',
            'Failed %s metadata extraction from %s', name, ds,
        )
        raise ValueError(
            "Failed to load metadata extractor for '%s', "
            "broken dataset configuration (%s)?: %s",
            name, ds, exc_str(e))

    # this is the old way of extractor operation
    dsmeta_t, contentmeta_t = extractor.get_metadata(
        dataset=want_dataset_meta,
        content=want_content_meta,
    )
    # fake the new way of reporting results directly
    # extractors had no way to report errors, hence
    # everything is unconditionally 'ok'
    for loc, meta in contentmeta_t or []:
        yield dict(
            status='ok',
            path=loc,
            type='file',
            metadata=meta,
        )
    yield dict(
        status='ok',
        path=ds.path,
        type='dataset',
        metadata=dsmeta_t,
    )


def _ok_metadata(res, msrc, ds, loc):
    restype = res.get('type', None)
    if restype not in ('dataset', 'file'):  # pragma: no cover
        # untested, would need broken extractor
        lgr.error(
            'metadata report for something other than a file or dataset: %s',
            restype
        )
        return False

    meta = res.get('metadata', None)
    if meta is None or isinstance(meta, dict):
        return True
    else:  # pragma: no cover
        # untested, needs broken extract
        # extractor
        msg = (
            "Metadata extractor '%s' yielded something other than a "
            "dictionary for dataset %s%s -- this is likely a bug, "
            "please consider reporting it. "
            "This type of native metadata will be ignored. Got: %s",
            msrc,
            ds,
            '' if loc is None else ' content {}'.format(loc),
            repr(meta))
        if cfg.get('datalad.runtime.raiseonerror'):
            raise RuntimeError(*msg)

        lgr.error(*msg)
        return False
"""
