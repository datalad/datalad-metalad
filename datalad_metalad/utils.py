import os.path as op
from collections import OrderedDict
from six import text_type

from datalad.utils import Path

from datalad.distribution.dataset import (
    rev_get_dataset_root,
    rev_resolve_path,
)

import logging
lgr = logging.getLogger('datalad.dataset')


# TODO drop when https://github.com/datalad/datalad/pull/3247
# is merged
def sort_paths_by_datasets(orig_dataset_arg, paths):
    """Sort paths into actually present datasets

    Parameters
    ----------
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
        p = rev_resolve_path(p, orig_dataset_arg)
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
            if orig_dataset_arg and root == text_type(p) and \
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
