from pathlib import Path
from typing import (
    List,
    Optional,
    Union,
)
from uuid import UUID

from datalad.api import (
    create,
    meta_add,
)
from datalad.distribution.dataset import Dataset
from datalad.support.gitrepo import GitRepo
from datalad.tests.utils_pytest import assert_repo_status

from dataladmetadatamodel.metadatapath import MetadataPath


def create_dataset(directory: str, dataset_id: UUID) -> GitRepo:

    git_repo = GitRepo(directory)

    git_repo_path = Path(git_repo.path)
    datalad_dir = git_repo_path / ".datalad"
    datalad_dir.mkdir()

    datalad_config = datalad_dir / "config"
    datalad_config.write_text(
        '[datalad "dataset"]\n'
        f'\tid = {dataset_id}')

    return git_repo


def create_dataset_proper(directory: Union[str, Path],
                          sub_dataset_names: Optional[List[Union[str, Path]]] = None
                          ) -> Dataset:

    directory = Path(directory)
    ds = Dataset(directory).create(force=True)
    ds.config.add(
        'datalad.metadata.exclude-path',
        '.metadata',
        scope='branch')
    ds.save(result_renderer="disabled")
    assert_repo_status(ds.path)

    sub_dataset_names = sub_dataset_names or []
    for sub_dataset_name in sub_dataset_names:
        create(directory/sub_dataset_name, dataset=directory)
    return ds


common_elements = {
    "extractor_name": "test_dataset",
    "extractor_version": "1.0",
    "extraction_parameter": {},
    "extraction_time": "1000.1",
    "agent_name": "test_aggregate",
    "agent_email": "test@test.aggregate"
}


def _get_base_elements(dataset_id: str,
                       dataset_version: str,
                       metadata_content: str,
                       root_dataset_id: Optional[str] = None,
                       root_dataset_version: Optional[str] = None,
                       dataset_path: Optional[str] = None) -> dict:

    if root_dataset_id is not None:
        assert root_dataset_version is not None
        assert dataset_path is not None
        root_info = {
            "root_dataset_id": root_dataset_id,
            "root_dataset_version": root_dataset_version,
            "dataset_path": str(dataset_path),
        }
    else:
        root_info = {}

    return {
        **common_elements,
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "extracted_metadata": {
            "content": metadata_content
        },
        **root_info
    }


def add_dataset_level_metadata(metadata_store: Path,
                               dataset_id: str,
                               dataset_version: str,
                               metadata_content: str,
                               root_dataset_id: Optional[str] = None,
                               root_dataset_version: Optional[str] = None,
                               dataset_path: Optional[str] = None):

    base_elements = _get_base_elements(
        dataset_id,
        dataset_version,
        metadata_content,
        root_dataset_id,
        root_dataset_version,
        dataset_path)

    meta_add({
            **base_elements,
            "type": "dataset"
        },
        dataset=metadata_store,
        result_renderer="disabled")


def add_file_level_metadata(metadata_store: Path,
                            dataset_id: str,
                            dataset_version: str,
                            file_path: MetadataPath,
                            metadata_content: str,
                            root_dataset_id: Optional[str] = None,
                            root_dataset_version: Optional[str] = None,
                            dataset_path: Optional[str] = None):

    base_elements = _get_base_elements(
        dataset_id,
        dataset_version,
        metadata_content,
        root_dataset_id,
        root_dataset_version,
        dataset_path)

    meta_add({
            **base_elements,
            "type": "file",
            "path": str(file_path)
        },
        dataset=metadata_store,
        result_renderer="disabled")
