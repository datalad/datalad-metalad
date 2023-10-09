from datalad.api import (
    Dataset,
    save,
)
from datalad.tests.utils_pytest import (
    create_tree,
)


def make_ds_hierarchy_with_metadata(path):
    """Test helper that returns the two datasets in the hierarchy

    The top-level dataset contains an annex'ed file with annex
    metadata.
    """
    ds = Dataset(path).create(force=True)
    create_tree(ds.path, {'file.dat': 'content'})
    ds.save(result_renderer="disabled")
    ds.repo.set_metadata('file.dat', reset={'tag': ['one', 'two']})
    subds = ds.create('sub')
    # we need one real piece of content for metadata extraction
    (subds.pathobj / 'real').write_text('real')
    ds.save(recursive=True, result_renderer="disabled")
    return ds, subds


def _get_dsid_from_core_metadata(md):
    return _get_dsmeta_from_core_metadata(md)['@id']


def _get_dsmeta_from_core_metadata(md):
    docs = md['@graph'] if '@graph' in md else [md]
    for doc in docs:
        if doc.get('@type', None) == 'Dataset':
            return doc
