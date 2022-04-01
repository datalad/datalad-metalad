# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""
Run a metadata filter on a set of metadata elements
"""
import json
import logging
from pathlib import Path
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

from datalad.distribution.dataset import Dataset
from datalad.distribution.dataset import (
    datasetmethod,
    EnsureDataset,
)
from datalad.interface.base import Interface
from datalad.interface.base import build_doc
from datalad.interface.utils import eval_results
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr,
)
from datalad.support.param import Parameter
from datalad.ui import ui

from .dump import (
    dump_from_dataset_tree,
    dump_from_uuid_set,
)
from .filters.base import MetadataFilterBase
from .metadatatypes.metadata import (
    MetadataRecord,
    MetadataResult,
    META_FILTER,
)
from .metadatatypes.result import (
    OK,
)
from .metadatautils import get_metadata_objects
from .pathutils.metadataurlparser import (
    MetadataURL,
    TreeMetadataURL,
    UUIDMetadataURL,
    parse_metadata_url,
)


__docformat__ = "restructuredtext"


default_backend = "git"

lgr = logging.getLogger("datalad.metadata.filter")


def create_metadata_object(metadata_dict: dict) -> MetadataRecord:
    """Create a metadata type instance from a JSON representation """
    return MetadataRecord.from_json(metadata_dict)


def create_iterator(dataset: Union[str, Path],
                    metadata_url: MetadataURL,
                    recursive: bool) -> Iterable:

    metadata_store_path, tree_version_list, uuid_set = get_metadata_objects(
        dataset=dataset,
        backend=default_backend
    )

    if isinstance(metadata_url, UUIDMetadataURL):
        for metadata_info in dump_from_uuid_set(mapper=default_backend,
                                                metadata_store=metadata_store_path,
                                                uuid_set=uuid_set,
                                                path=metadata_url,
                                                recursive=recursive):
            yield create_metadata_object(metadata_info["metadata"])

    elif isinstance(metadata_url, TreeMetadataURL):
        for metadata_info in dump_from_dataset_tree(mapper=default_backend,
                                                    metadata_store=metadata_store_path,
                                                    tree_version_list=tree_version_list,
                                                    metadata_url=metadata_url,
                                                    recursive=recursive):
            yield create_metadata_object(metadata_info["metadata"])

    else:
        raise ValueError(
            f"unsupported metadata url type: {type(metadata_url).__name__}")


@build_doc
class Filter(Interface):
    """Run a metadata filter on a set of metadata elements.

    Take a number of metadata elements and run a filter on it.

    The result of the filter operation will be written to stdout and can, for
    example, be passed to meta-add.

    The filter can be configured by passing key-value pairs given as additional
    arguments. Each key-value pair consists of two arguments, first the key,
    then the value. The key value pairs have to be separated by '++' from the
    metadata coordinates
    """

    result_renderer = "tailored"

    _examples_ = [
        dict(
            text="""Use the provided 'metalad_demofilter' to build a
            'histogram' of keys and their content in the metadata of the
            dataset 'root-dataset', iterating over the sub-datasets 'sub-a' and 
            'sub-b'.""",
            code_cmd="""datalad meta-filter metalad_demofilter -d root-dataset
            sub-a sub-b"""
        ),
        dict(
            text="""Apply 'metalad_demofilter' to all directories/sub-datasets
            of the dataset in the current working directory that start with
            'subject'.""",
            code_cmd="""datalad meta-filter metalad_demofilter subject*"""
        ),
    ]

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""Git repository that contains datalad metadata. If no
                   repository path is given, the metadata store is determined
                   by the current work directory. All metadata URLs (see below)
                   are relative to this dataset.""",
            constraints=EnsureDataset() | EnsureNone()),
        filtername=Parameter(
            args=("filtername",),
            metavar="FILTER_NAME",
            doc="Name of the filter that should be executed.",
            constraints=EnsureStr()),
        metadataurls=Parameter(
            args=("metadataurls",),
            metavar="METADATA_URL",
            nargs="+",
            doc="""MetadataRecord URL(s). A list of at least one metadata URL.
                   The filter will receive a list of iterables, that contains
                   one iterable for each metadata URL. The iterables will yields
                   all metadata-entries that match the respective metadata URL.
                   """,
            constraints=EnsureStr()),
        # TODO: this parameter is specified here in order to print out a
        #  proper help message. It will never be filled by the argument parser
        #  because "metadataurls" has an arbitrary number of arguments, that
        #  means: "metadataurls" will eat up all "filterargs".
        filterargs=Parameter(
            args=("filterargs",),
            metavar="FILTER_ARGUMENTS",
            doc="""Extractor arguments given as string arguments to the
                   extractor. Filter arguments have to be separated from the
                   list of metadata coordinates by '++'.""",
            nargs="*",
            constraints=EnsureStr() | EnsureNone()),
        recursive=Parameter(
            args=("-r", "--recursive",),
            action="store_true",
            doc="""If set, the metadata URL iterables will yield all metadata
                   recursively from the matching metadata URLs."""))

    @staticmethod
    @datasetmethod(name="meta_filter")
    @eval_results
    def __call__(
            filtername: str,
            metadataurls: List[Union[str, MetadataURL]],
            dataset: Optional[Union[Dataset, str]] = ".",
            filterargs: Optional[List[str]] = None,
            recursive: bool = False) -> Iterable:

        # Get basic arguments
        filter_name = filtername
        if '++' in metadataurls:
            plusplus_index = metadataurls.index('++')
            metadata_urls, filter_args = (
                metadataurls[:plusplus_index],
                metadataurls[plusplus_index + 1:]
            )
        else:
            metadata_urls, filter_args = (
                metadataurls,
                filterargs or []
            )

        if not metadata_urls:
            raise ValueError("At least one metadata URL is required")

        metadata_urls = [
            url if isinstance(url, MetadataURL) else parse_metadata_url(url)
            for url in metadata_urls
        ]

        metadata_iterables = [
            create_iterator(dataset, metadata_url, recursive)
            for metadata_url in metadata_urls
        ]

        path = (
            dataset.pathobj
            if isinstance(dataset, Dataset)
            else Path(dataset)
        )
        if not path.is_absolute():
            path = Path.cwd() / path

        for metadata_record in run_filter(filter_name=filter_name,
                                          filter_args=filter_args,
                                          metadata_iterables=metadata_iterables):

            yield MetadataResult(
                status=OK,
                path=path,
                action=META_FILTER,
                metadata_type=metadata_record.type,
                metadata_record=metadata_record,
                metadata_source=path,
                backend="git").as_json_obj()

    @staticmethod
    def custom_result_renderer(res, **kwargs):
        if res["status"] != "ok" or res.get("action", "") != 'meta_filter':
            # logging complained about this already
            return

        metadata_record = res.get("metadata_record", None)
        if metadata_record is not None:
            path = (
                {"path": str(metadata_record["path"])}
                if "path" in metadata_record
                else {}
            )

            dataset_path = (
                {"dataset_path": str(metadata_record["dataset_path"])}
                if "dataset_path" in metadata_record
                else {}
            )

            ui.message(json.dumps({
                **metadata_record,
                **path,
                **dataset_path,
                "dataset_id": str(metadata_record["dataset_id"])
            }))

        context = res.get("context")
        if context is not None:
            ui.message(json.dumps(context))


def run_filter(filter_name: str,
               filter_args: Optional[List],
               metadata_iterables: List[Iterable]
               ) -> Iterable[MetadataRecord]:

    filter_class = get_filter_class(filter_name)
    filter_instance = filter_class(filter_name)
    args, kwargs = split_arguments(filter_args, filter_class, filter_instance)
    yield from filter_instance.filter(
        metadata_iterables,
        *(args or []),
        **(kwargs or {}))


def get_filter_class(filter_name: str) -> Type[MetadataFilterBase]:
    """ Get a filter class from its name"""
    from pkg_resources import iter_entry_points

    entry_points = list(
        iter_entry_points("datalad.metadata.filters", filter_name))

    if not entry_points:
        raise ValueError(
            "Requested metadata filter '{}' not available".format(
                filter_name))

    entry_point, ignored_entry_points = entry_points[-1], entry_points[:-1]
    lgr.debug(
        "Using metadata filter %s from distribution %s",
        filter_name,
        entry_point.dist.project_name)

    # Inform about overridden entry points
    for ignored_entry_point in ignored_entry_points:
        lgr.warning(
            "MetadataRecord filter %s from distribution %s overrides "
            "metadata filter from distribution %s",
            filter_name,
            entry_point.dist.project_name,
            ignored_entry_point.dist.project_name)

    return entry_point.load()


def split_arguments(args: List[str],
                    filter_class: Type,
                    filter_instance: object
                    ) -> Tuple[List, Dict]:
    """
    Split arguments into positional arguments and keyword arguments.
    TODO: Splitting is currently based on the presence of "=" in the argument.
          It should instead be based on a specification of arguments in the
          class or in the instance.

    :param args: a list of arguments
    :param filter_class:
           the class of the filter that should receive the arguments
    :param filter_instance:
           the instance of the filter that should receive the arguments
    :return:
           a tuple consisting of a list of positional arguments and a dictionary
           of keyword arguments
    """
    filter_args = list(filter(lambda argument: "=" not in argument, args))
    filter_kwargs = {
        argument.split("=", maxsplit=1)[0]: argument.split("=", maxsplit=1)[1:]
        for argument in args
        if "=" in argument}

    return filter_args, filter_kwargs
