# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test metadata extraction"""
import subprocess
import unittest.mock
from pathlib import Path
from typing import (
    IO,
    Optional,
    Union,
)
from unittest.mock import patch
from uuid import UUID

from datalad.api import (
    create,
    meta_extract,
)
from datalad.distribution.dataset import Dataset
from datalad.support.exceptions import NoDatasetFound
from datalad.tests.utils_pytest import (
    assert_cwd_unchanged,
    assert_in,
    assert_repo_status,
    assert_raises,
    assert_result_count,
    assert_true,
    eq_,
    known_failure_windows,
    with_tempfile,
    with_tree
)
from datalad.utils import chpwd
from dataladmetadatamodel.metadatapath import MetadataPath

from .utils import create_dataset
from ..exceptions import ExtractorNotFoundError
from ..extract import get_extractor_class
from ..extractors.base import (
    DatasetMetadataExtractor,
    DataOutputCategory,
    ExtractorResult,
)


meta_tree = {
    "sub": {
        "one": "1",
        "nothing": "2",
    },
}

common_kwargs = dict(result_renderer="disabled")


def _create_dataset_at_path(ds_path):
    ds = Dataset(ds_path).create(force=True)
    ds.config.add(
        'datalad.metadata.exclude-path',
        '.metadata',
        scope='branch')
    ds.save(**common_kwargs)
    assert_repo_status(ds.path)
    return ds


@with_tempfile(mkdir=True)
def test_empty_dataset_error(path=None):
    # go into virgin dir to avoid detection of any dataset
    with chpwd(path):
        assert_raises(
            NoDatasetFound,
            meta_extract,
            extractorname="metalad_core")


@with_tempfile(mkdir=True)
def test_unknown_extractor_error(path=None):
    # ensure failure on unavailable metadata extractor
    create_dataset(path, UUID(int=0))
    with chpwd(path):
        assert_raises(
            ExtractorNotFoundError,
            meta_extract,
            extractorname="bogus__")


def _check_metadata_record(metadata_record: dict,
                           dataset: Dataset,
                           extractor_name: str,
                           extractor_version: str,
                           extraction_parameter: dict,
                           path: Optional[str] = None):

    assert_in("extraction_time", metadata_record)
    eq_(metadata_record["dataset_id"], UUID(dataset.id))
    eq_(metadata_record["dataset_version"], dataset.repo.get_hexsha())
    eq_(metadata_record["extractor_version"], extractor_version)
    eq_(metadata_record["extractor_name"], extractor_name)
    eq_(metadata_record["extraction_parameter"], extraction_parameter)
    eq_(metadata_record["agent_name"], dataset.config.get("user.name"))
    eq_(metadata_record["agent_email"], dataset.config.get("user.email"))
    if path is not None:
        eq_(metadata_record["path"], MetadataPath(path))


@with_tree(meta_tree)
def test_dataset_extraction_result(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    extractor_name = "metalad_example_dataset"
    extractor_class = get_extractor_class(extractor_name)
    extractor_version = extractor_class(None, None, None).get_version()

    res = meta_extract(
        extractorname=extractor_name,
        dataset=ds,
        **common_kwargs)

    assert_result_count(res, 1)
    assert_result_count(res, 1, type='dataset')
    assert_result_count(res, 0, type='file')

    metadata_record = res[0]["metadata_record"]
    _check_metadata_record(
        metadata_record=metadata_record,
        dataset=ds,
        extractor_name=extractor_name,
        extractor_version=extractor_version,
        extraction_parameter={})

    extracted_metadata = metadata_record["extracted_metadata"]
    eq_(extracted_metadata["id"], ds.id)
    eq_(extracted_metadata["refcommit"], ds.repo.get_hexsha())
    assert_true(extracted_metadata["comment"].startswith(
        "example dataset extractor executed at "))


@with_tree(meta_tree)
def test_file_extraction_result(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    file_path = "sub/one"
    extractor_name = "metalad_example_file"
    extractor_class = get_extractor_class(extractor_name)
    extractor_version = extractor_class(None, None, None).get_version()

    res = meta_extract(
        extractorname=extractor_name,
        path=file_path,
        dataset=ds,
        **common_kwargs)

    assert_result_count(res, 1)
    assert_result_count(res, 1, type='file')
    assert_result_count(res, 0, type='dataset')

    metadata_record = res[0]["metadata_record"]
    _check_metadata_record(
        metadata_record=metadata_record,
        dataset=ds,
        extractor_name=extractor_name,
        extractor_version=extractor_version,
        extraction_parameter={},
        path=file_path)

    extracted_metadata = metadata_record["extracted_metadata"]
    assert_in("content_byte_size", extracted_metadata)
    assert_in("@id", extracted_metadata)
    eq_(extracted_metadata["type"], "file")
    eq_(extracted_metadata["path"], file_path)
    assert_true(extracted_metadata["comment"].startswith(
        "example file extractor executed at "))


@with_tree(meta_tree)
def test_legacy1_dataset_extraction_result(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    extractor_name = "metalad_core"
    extractor_version = "1"

    res = meta_extract(
        extractorname=extractor_name,
        dataset=ds,
        **common_kwargs)

    assert_result_count(res, 1)
    assert_result_count(res, 1, type='dataset')
    assert_result_count(res, 0, type='file')

    metadata_record = res[0]["metadata_record"]
    _check_metadata_record(
        metadata_record=metadata_record,
        dataset=ds,
        extractor_name=extractor_name,
        extractor_version=extractor_version,
        extraction_parameter={})

    extracted_metadata = metadata_record["extracted_metadata"]
    assert_in("@context", extracted_metadata)
    assert_in("@graph", extracted_metadata)
    eq_(len(extracted_metadata["@graph"]), 2)


@with_tree(meta_tree)
def test_legacy2_dataset_extraction_result(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    extractor_name = "datalad_core"
    extractor_version = "un-versioned"

    res = meta_extract(
        extractorname=extractor_name,
        dataset=ds,
        **common_kwargs)

    assert_result_count(res, 1)
    assert_result_count(res, 1, type='dataset')
    assert_result_count(res, 0, type='file')

    metadata_record = res[0]["metadata_record"]
    _check_metadata_record(
        metadata_record=metadata_record,
        dataset=ds,
        extractor_name=extractor_name,
        extractor_version=extractor_version,
        extraction_parameter={})

    extracted_metadata = metadata_record["extracted_metadata"]
    assert_in("@id", extracted_metadata)


@with_tree(meta_tree)
def test_legacy1_file_extraction_result(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    file_path = "sub/one"
    extractor_name = "metalad_core"
    extractor_version = "1"

    res = meta_extract(
        extractorname=extractor_name,
        path=file_path,
        dataset=ds,
        **common_kwargs)

    assert_result_count(res, 1)
    assert_result_count(res, 1, type='file')
    assert_result_count(res, 0, type='dataset')

    metadata_record = res[0]["metadata_record"]
    _check_metadata_record(
        metadata_record=metadata_record,
        dataset=ds,
        extractor_name=extractor_name,
        extractor_version=extractor_version,
        extraction_parameter={},
        path=file_path)

    extracted_metadata = metadata_record["extracted_metadata"]
    assert_in("@id", extracted_metadata)
    eq_(extracted_metadata["contentbytesize"], 1)


@known_failure_windows
@with_tree(meta_tree)
def test_legacy2_file_extraction_result(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    file_path = "sub/one"
    extractor_name = "datalad_core"
    extractor_version = "un-versioned"

    res = meta_extract(
        extractorname=extractor_name,
        path=file_path,
        dataset=ds,
        **common_kwargs)

    assert_result_count(res, 1)
    assert_result_count(res, 1, type='file')
    assert_result_count(res, 0, type='dataset')

    metadata_record = res[0]["metadata_record"]
    _check_metadata_record(
        metadata_record=metadata_record,
        dataset=ds,
        extractor_name=extractor_name,
        extractor_version=extractor_version,
        extraction_parameter={},
        path=file_path)

    extracted_metadata = metadata_record["extracted_metadata"]
    eq_(extracted_metadata, {})


@with_tree(meta_tree)
def test_path_parameter_directory(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    assert_raises(
        ValueError,
        meta_extract,
        extractorname="metalad_example_file",
        dataset=ds,
        path="sub")


@with_tree(meta_tree)
def test_path_parameter_recognition(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    with patch("datalad_metalad.extract.do_file_extraction") as fe, \
         patch("datalad_metalad.extract.do_dataset_extraction") as de:

        meta_extract(
            extractorname="metalad_example_file",
            dataset=ds,
            path="sub/one",
            **common_kwargs)
        eq_(fe.call_count, 1)
        eq_(de.call_count, 0)


@with_tree(meta_tree)
def test_extra_parameter_recognition(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    with patch("datalad_metalad.extract.do_file_extraction") as fe, \
         patch("datalad_metalad.extract.do_dataset_extraction") as de:

        meta_extract(
            extractorname="metalad_example_file",
            dataset=ds,
            force_dataset_level=True,
            path="k1",
            extractorargs=["v1", "k2", "v2", "k3", "v3"],
            **common_kwargs)

        eq_(fe.call_count, 0)
        eq_(de.call_count, 1)
        eq_(
            de.call_args_list[0][0][0].extraction_parameter,
            {
                "k1": "v1",
                "k2": "v2",
                "k3": "v3"
            })


@with_tree(meta_tree)
def test_path_and_extra_parameter_recognition(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    with patch("datalad_metalad.extract.do_file_extraction") as fe, \
         patch("datalad_metalad.extract.do_dataset_extraction") as de:

        meta_extract(
            extractorname="metalad_example_file",
            dataset=ds,
            path="sub/one",
            extractorargs=["k1", "v1", "k2", "v2", "k3", "v3"],
            **common_kwargs)

        eq_(de.call_count, 0)
        eq_(fe.call_count, 1)
        eq_(
            fe.call_args_list[0][0][0].extraction_parameter,
            {
                "k1": "v1",
                "k2": "v2",
                "k3": "v3"
            })


@with_tree(meta_tree)
def test_context_dict_parameter_handling(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    with patch("datalad_metalad.extract.do_file_extraction") as fe, \
         patch("datalad_metalad.extract.do_dataset_extraction") as de:

        meta_extract(
            extractorname="metalad_example_file",
            dataset=ds,
            context={"dataset_version": "xyz"},
            path="sub/one",
            **common_kwargs)

        eq_(fe.call_count, 1)
        eq_(fe.call_args[0][0].source_dataset_version, "xyz")
        eq_(de.call_count, 0)


@with_tree(meta_tree)
def test_context_str_parameter_handling(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    with patch("datalad_metalad.extract.do_file_extraction") as fe, \
         patch("datalad_metalad.extract.do_dataset_extraction") as de:

        meta_extract(
            extractorname="metalad_example_file",
            dataset=ds,
            context='{"dataset_version": "rst"}',
            path="sub/one",
            **common_kwargs)

        eq_(fe.call_count, 1)
        eq_(fe.call_args[0][0].source_dataset_version, "rst")
        eq_(de.call_count, 0)


@with_tree(meta_tree)
def test_get_context(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    result = tuple(
        meta_extract(
            extractorname="metalad_example_file",
            dataset=ds,
            get_context=True,
            path="sub/one",
            **common_kwargs))

    version = subprocess.run(
        [
            "git",
             "--git-dir", str(ds.pathobj / ".git"),
             "log",
             "--oneline",
             "--format=%H",
             "-n", "1"
        ],
        stdout=subprocess.PIPE).stdout.decode().strip()
    eq_(len(result), 1)
    eq_(result[0]["context"]["dataset_version"], version)


@with_tree(meta_tree)
def test_extractor_parameter_handling(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    with patch("datalad_metalad.extract.do_file_extraction") as fe, \
         patch("datalad_metalad.extract.do_dataset_extraction") as de:

        meta_extract(
            extractorname="metalad_example_dataset",
            dataset=ds,
            force_dataset_level=True,
            path="k0",
            extractorargs=["v0", "k1", "v1"],
            **common_kwargs)

        eq_(fe.call_count, 0)
        eq_(de.call_count, 1)
        eq_(de.call_args[0][0].extraction_parameter, {"k0": "v0", "k1": "v1"})

    with patch("datalad_metalad.extract.do_file_extraction") as fe, \
            patch("datalad_metalad.extract.do_dataset_extraction") as de:

        meta_extract(
            extractorname="metalad_example_file",
            dataset=ds,
            path="sub/one",
            extractorargs=["k0", "v0", "k1", "v1"],
            **common_kwargs)

        eq_(de.call_count, 0)
        eq_(fe.call_count, 1)
        eq_(fe.call_args[0][0].file_tree_path, MetadataPath("sub/one"))
        eq_(fe.call_args[0][0].extraction_parameter, {"k0": "v0", "k1": "v1"})


@with_tree(meta_tree)
def test_external_extractor(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    for path, extractor_name in ((None, "metalad_external_dataset"),
                                 ("sub/one", "metalad_external_file")):
        result = meta_extract(
            extractorname=extractor_name,
            dataset=ds,
            path=path,
            extractorargs=[
                "data-output-category", "IMMEDIATE",
                "command", ["python", "-c", "print('True')"],
                "arguments", ["-c", "print('True')"]
            ],
            **common_kwargs)
        eq_(len(result), 1)
        eq_(result[0]["status"], "ok")
        eq_(result[0]["metadata_record"]["extracted_metadata"], "True")


@with_tree(meta_tree)
def test_external_extractor_categories(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    for path, extractor_name in ((None, "metalad_external_dataset"),
                                 ("sub/one", "metalad_external_file")):
        for output_category in ("DIRECTORY", "FILE"):
            assert_raises(
                NotImplementedError,
                meta_extract,
                extractorname=extractor_name,
                dataset=ds,
                path=path,
                extractorargs=[
                    "data-output-category", output_category,
                    "command", ["python", "-c", "print('True')"]
                ],
                **common_kwargs)


@with_tree(meta_tree)
def test_get_required_content_called(ds_path=None):

    ds = _create_dataset_at_path(ds_path)

    class TestExtractor(DatasetMetadataExtractor):
        def __init__(self, dataset, ref_commit, parameter):
            DatasetMetadataExtractor.__init__(
                self,
                dataset,
                ref_commit,
                parameter)

            self.required_content_called = False

        def get_required_content(self):
            self.required_content_called = True

        def get_id(self) -> UUID:
            return UUID(int=10)

        def get_version(self):
            return "0.0.1"

        def get_data_output_category(self) -> DataOutputCategory:
            return DataOutputCategory.IMMEDIATE

        def extract(self,
                    output_location: Optional[Union[IO, str]] = None
                    ) -> ExtractorResult:
            return ExtractorResult(
                extractor_version="0.0.1",
                extraction_parameter={},
                extraction_success=self.required_content_called,
                datalad_result_dict={
                    "status": "ok",
                    "action": "extract",
                },
                immediate_data={
                    "required_content_called": self.required_content_called
                }
            )

    with patch("datalad_metalad.extract.get_extractor_class") as gec_mock:
        gec_mock.return_value = TestExtractor
        result = meta_extract(
            extractorname="test_name",
            dataset=ds,
            path=None,
            extractorargs=["k0", "v0", "k1", "v1"],
            **common_kwargs)
        assert_true(
            result[0]
            ["metadata_record"]
            ["extracted_metadata"]
            ["required_content_called"])


@with_tempfile(mkdir=True)
@assert_cwd_unchanged()# ok_to_chdir=True)
def test_path_assembly(temp_dir=None):

    def check_with_file_path(dfe_mock: type(unittest.mock.MagicMock),
                             file_path: Path,
                             expected_path: str):
        meta_extract(
            dataset="..",
            extractorname="metalad_core",
            path=str(file_path)
        )
        extraction_arguments = dfe_mock.call_args[0][0]
        eq_(extraction_arguments.local_source_object_path, file_path.absolute())
        eq_(extraction_arguments.file_tree_path, MetadataPath(expected_path))

    ds_path = Path(temp_dir) / "dataset"
    ds = Dataset(ds_path).create()
    subdir_path = ds_path / "sub1"
    subdir_path.mkdir()
    file_name = "info.txt"
    file_path = subdir_path / file_name
    file_path.write_text("some content")
    ds.save(**common_kwargs)

    with chpwd(str(subdir_path)):
        with patch("datalad_metalad.extract.do_file_extraction") as dfe_mock:
            # Check absolute path
            check_with_file_path(dfe_mock, file_path.absolute(), "sub1/info.txt")
            check_with_file_path(dfe_mock, file_path, "sub1/info.txt")


@with_tempfile(mkdir=True)
def test_not_tracked_error_catching(temp_dir=None):
    # expect a value error, if the provided file is not tracked.
    ds_path = Path(temp_dir) / "dataset"
    ds = Dataset(ds_path).create()
    file_name = "info.txt"
    file_path = ds.pathobj / file_name
    file_path.write_text("some content")
    ds.save(**common_kwargs)

    assert_raises(
        ValueError,
        meta_extract,
        dataset=ds,
        extractorname="metalad_core",
        path="no_such_file.txt"
    )


@with_tempfile(mkdir=True)
def test_symlink_handling(tmp_dir_path_str=None):
    tmp_dir = Path(tmp_dir_path_str)

    super_ds = create(tmp_dir / "super")
    super_ds.save(**common_kwargs)
    create(
        dataset=str(tmp_dir / "super"),
        path=str(tmp_dir / "super" / "sub"),
    )
    super_ds.save(recursive=True, **common_kwargs)

    (tmp_dir / "tmp").mkdir()
    (tmp_dir / "tmp" / "link").symlink_to(tmp_dir / "super")

    result = tuple(
        meta_extract(
            dataset=str(tmp_dir / "tmp" / "link"),
            extractorname="metalad_core",
            **common_kwargs,
        )
    )
