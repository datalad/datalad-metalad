from pathlib import Path
from typing import Optional
from uuid import UUID

from dataladmetadatamodel.metadatapath import MetadataPath

from datalad.api import meta_add
from datalad.support.gitrepo import GitRepo


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
        dataset=metadata_store)


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
        dataset=metadata_store)