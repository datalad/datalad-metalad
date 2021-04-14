import os.path as op
from collections import OrderedDict
from itertools import islice
from six import text_type
from typing import Dict, List

from datalad.utils import Path

from datalad.distribution.dataset import (
    rev_get_dataset_root,
    resolve_path,
)

from . import aggregate_layout_version

import logging
lgr = logging.getLogger('datalad.dataset')


# TODO drop when https://github.com/datalad/datalad/pull/3247
# is merged
def sort_paths_by_datasets(refds, orig_dataset_arg, paths):
    """Sort paths into actually present datasets

    Parameters
    ----------
    refds : Dataset or None
      Dataset instance of a reference dataset, if any exists. This is
      not just a `dataset` argument in any form (for path resolution),
      see `orig_dataset_arg` for that, but has to be a Dataset instance
      that serves as the root of all operations.
    orig_dataset_arg : None or str
      The original dataset argument of the calling command. This is
      used to determine the path specification semantics, i.e.
      relative to CWD vs. relative to a given dataset
    paths : list
      Paths as given to the calling command

    Returns
    -------
    OrderedDict, list
      The dictionary contains all to-be-sorted paths as values to
      their respective containing datasets paths (as keys). The second
      list contains status dicts for any errors that may have occurred
      during processing. They can be yielded in the context of
      the calling command.
    """
    errors = []
    paths_by_ds = OrderedDict()
    # sort any path argument into the respective subdatasets
    for p in sorted(paths):
        # it is important to capture the exact form of the
        # given path argument, before any normalization happens
        # for further decision logic below
        orig_path = text_type(p)
        p = resolve_path(p, orig_dataset_arg)
        root = rev_get_dataset_root(text_type(p))
        if root is None:
            # no root, not possibly underneath the refds
            errors.append(dict(
                action='status',
                path=p,
                status='error',
                message='path not underneath this dataset',
                logger=lgr))
            continue
        else:
            if refds and root == text_type(p) and \
                    not orig_path.endswith(op.sep):
                # the given path is pointing to a dataset
                # distinguish rsync-link syntax to identify
                # the dataset as whole (e.g. 'ds') vs its
                # content (e.g. 'ds/')
                super_root = rev_get_dataset_root(op.dirname(root))
                if super_root:
                    # the dataset identified by the path argument
                    # is contained in a superdataset, and no
                    # trailing path separator was found in the
                    # argument -> user wants to address the dataset
                    # as a whole (in the superdataset)
                    root = super_root

        root = Path(root)
        ps = paths_by_ds.get(root, [])
        ps.append(p)
        paths_by_ds[root] = ps

    return paths_by_ds, errors


def get_ds_aggregate_db_locations(dspath, version='default', warn_absent=True):
    """Returns the location of a dataset's aggregate metadata DB

    Parameters
    ----------
    dspath : Path
      Path to a dataset to query
    version : str
      DataLad aggregate metadata layout version. At the moment only a single
      version exists. 'default' will return the locations for the current
      default layout version.
    warn_absent : bool
      If True, warn if the desired DB version is not present and give hints on
      what else is available. This is useful when using this function from
      a user-facing command.

    Returns
    -------
    db_location, db_object_base_path
      Absolute paths to the DB itself, and to the basepath to resolve relative
      object references in the database. Either path may not exist in the
      queried dataset.
    """
    layout_version = aggregate_layout_version \
        if version == 'default' else version

    agginfo_relpath_template = op.join(
        '.datalad',
        'metadata',
        'aggregate_v{}.json')
    agginfo_relpath = agginfo_relpath_template.format(layout_version)
    info_fpath = dspath / agginfo_relpath
    agg_base_path = info_fpath.parent
    # not sure if this is the right place with these check, better move then to
    # a higher level
    if warn_absent and not info_fpath.exists():  # pragma: no cover
        # legacy code from a time when users could not be aware of whether
        # they were doing metadata extraction, or querying aggregated metadata
        # nowadays, and error is triggered long before it reaches this code
        if version == 'default':
            from datalad.consts import (
                OLDMETADATA_DIR,
                OLDMETADATA_FILENAME,
            )
            # caller had no specific idea what metadata version is
            # needed/available This dataset does not have aggregated metadata.
            # Does it have any other version?
            info_glob = op.join(
                text_type(dspath), agginfo_relpath_template).format('*')
            info_files = glob.glob(info_glob)
            msg = "Found no aggregated metadata info file %s." \
                  % info_fpath
            old_metadata_file = op.join(
                text_type(dspath), OLDMETADATA_DIR, OLDMETADATA_FILENAME)
            if op.exists(old_metadata_file):
                msg += " Found metadata generated with pre-0.10 version of " \
                       "DataLad, but it will not be used."
            upgrade_msg = ""
            if info_files:
                msg += " Found following info files, which might have been " \
                       "generated with newer version(s) of datalad: %s." \
                       % (', '.join(info_files))
                upgrade_msg = ", upgrade datalad"
            msg += " You will likely need to either update the dataset from its " \
                   "original location%s or reaggregate metadata locally." \
                   % upgrade_msg
            lgr.warning(msg)
    return info_fpath, agg_base_path


def get_ds_aggregate_db(dspath, version='default', warn_absent=True):
    """Load a dataset's aggregate metadata database

    Parameters
    ----------
    dspath : Path
      Path to a dataset to query
    version : str
      DataLad aggregate metadata layout version. At the moment only a single
      version exists. 'default' will return the content of the current default
      aggregate database version.
    warn_absent : bool
      If True, warn if the desired DB version is not present and give hints on
      what else is available. This is useful when using this function from
      a user-facing command.

    Returns
    -------
    dict
      A dictionary with the database content is returned. All paths in the
      dictionary (datasets, metadata object archives) are
      absolute.
    """
    info_fpath, agg_base_path = get_ds_aggregate_db_locations(
        dspath, version, warn_absent)

    # save to call even with a non-existing location
    agginfos = json_load(text_type(info_fpath)) if info_fpath.exists() else {}

    return {
        # paths in DB on disk are always relative
        # make absolute to ease processing during aggregation
        dspath / p:
        {k: agg_base_path / v if k in location_keys else v
         for k, v in props.items()}
        for p, props in agginfos.items()
    }


def args_to_dict(args: List[str]) -> Dict[str, str]:
    """ Convert an argument list to a dictionary """

    if args is None:
        return {}

    if len(args) % 2 != 0:
        raise ValueError(
            f"argument list is missing value for key '{args[-1]}'")

    return dict(
        zip(
            islice(args, 0, len(args), 2),
            islice(args, 1, len(args), 2)))


def error_result(action: str, message: str, status: str = "error") -> dict:
    return dict(
        action=action,
        status="error",
        message=message)
