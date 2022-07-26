# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil; coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test all extractors at a basic level"""

from pkg_resources import iter_entry_points

import pytest

from datalad.api import (
    Dataset,
    meta_extract,
    save,
    install,
)
from datalad.support.gitrepo import GitRepo
from datalad.support.exceptions import NoDatasetFound
from datalad.tests.utils_pytest import (
    assert_repo_status,
    assert_true,
    assert_not_in,
    assert_in,
    assert_raises,
    assert_result_count,
    known_failure_windows,
    with_tree,
    with_tempfile,
)

from ...tests import (
    make_ds_hierarchy_with_metadata,
    _get_dsmeta_from_core_metadata,
)
from ...tests.utils import create_dataset_proper


@pytest.mark.parametrize("annex", [True, False])
@with_tree(tree={"file.dat": ""})
def test_api(path=None, *, annex):
    ds = Dataset(path).create(force=True, annex=annex)
    ds.save(result_renderer="disabled")
    assert_repo_status(ds.path)

    processed_extractors, skipped_extractors = [], []
    for extractor_ep in iter_entry_points("datalad.metadata.extractors"):

        # There are a number of extractors that do not
        # work on empty datasets, or datasets, or without
        # external processes. We skip those here, instead
        # of skipping them later.
        if extractor_ep.name in ("metalad_example_file",
                                 "external_dataset",
                                 "external_file",
                                 "metalad_studyminimeta",
                                 "bids",
                                 "dicom"):
            continue

        # we need to be able to query for metadata, even if there is none
        # from any extractor
        try:
            res = meta_extract(
                dataset=ds,
                extractorname=extractor_ep.name,
            )
        except Exception as exc:
            skipped_extractors += [f"{extractor_ep.name}: {exc}"]
            continue

        # metalad_core does provide some information about our precious file
        if extractor_ep.name == "metalad_core":
            assert_result_count(
                res,
                1,
                path=ds.path,
                type="dataset",
                status="ok",
            )

            assert_true(
                all([
                    r["metadata_record"]["extractor_name"] == extractor_ep.name
                    for r in res]))

            extracted_metadata = [
                r["metadata_record"]["extracted_metadata"]
                for r in res
            ]

            # every single report comes with an identifier
            assert_true(all(
                (em["@graph"][1]["identifier"] == ds.id
                 for em in extracted_metadata)
            ))

        processed_extractors.append(extractor_ep.name)
    assert "metalad_core" in processed_extractors, \
        "Should have managed to find at least the core extractor extractor"
    if skipped_extractors:
        pytest.skip(
            "Not fully tested/succeded since some extractors failed"
            " to load:\n%s" % ("\n".join(skipped_extractors)))


@with_tempfile(mkdir=True)
def test_plainest(path=None):

    # Expect an exception if no dataset exists. Depending on the
    # dataset version ValueError or NoDatasetFound is raised.
    from datalad import __version__
    major, minor, patch = tuple(
        map(
            lambda s: int(s.split("+")[0]),
            __version__.split(".")[0:3]
        )
    )

    if major == 0 and minor <= 15:
        expected_exception = ValueError
    else:
        expected_exception = NoDatasetFound

    assert_raises(
        expected_exception,
        meta_extract, dataset=path, extractorname="metalad_core")

    r = GitRepo(path, create=True)
    # Expect an exception, when the dataset is unusable because it has
    # no dataset id
    assert_raises(
        expected_exception,
        meta_extract, dataset=path, extractorname="metalad_core")

    # Expect proper extract run on a proper dataset.
    dataset = create_dataset_proper(path)
    assert_result_count(
        meta_extract(
            dataset=dataset.path,
            extractorname="metalad_core",
            on_failure="ignore",
        ),
        1,
        status="ok",
        # message contains exception
        type="dataset",
        path=r.path,
    )


@known_failure_windows
@with_tempfile
@with_tempfile
def test_report(path=None, orig=None):
    origds, subds = make_ds_hierarchy_with_metadata(orig)
    # now clone to a new place to ensure no content is present
    ds = install(source=origds.path, path=path)
    # only dataset-global metadata
    res = meta_extract(dataset=ds, extractorname="metalad_core")
    assert_result_count(res, 1)
    core_dsmeta = _get_dsmeta_from_core_metadata(
        res[0]["metadata_record"]["extracted_metadata"]
    )
    assert_in(
        {
            "@type": "Dataset",
            "@id": "datalad:{}".format(subds.repo.get_hexsha()),
            "identifier": "datalad:{}".format(subds.id),
            "name": "sub"
        },
        core_dsmeta["hasPart"]
    )
    # has not seen the content
    assert_not_in(
        "contentbytesize",
        core_dsmeta
    )
    res = meta_extract(dataset=ds,
                       extractorname="metalad_annex",
                       path="file.dat")
    assert(any(
        dict(tag=["one", "two"]) == r["metadata_record"]["extracted_metadata"]
        for r in res
    ))
    # we have a report on file(s)
    assert(len(res) > 0)
    # but no subdataset reports
    assert_result_count(res, 0, type="dataset")
