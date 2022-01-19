import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Dict,
    List,
    Tuple,
    Union,
    cast,
)
from uuid import UUID

from dataladmetadatamodel.datasettree import DatasetTree
from dataladmetadatamodel.filetree import FileTree
from dataladmetadatamodel.metadatarootrecord import MetadataRootRecord
from dataladmetadatamodel.metadata import (
    ExtractorConfiguration,
    Metadata,
)
from dataladmetadatamodel.metadatapath import MetadataPath
from dataladmetadatamodel.uuidset import UUIDSet
from dataladmetadatamodel.versionlist import (
    TreeVersionList,
    VersionList,
)


@dataclass(frozen=True)
class DatasetDescription:
    uuid: UUID
    version: str
    path: MetadataPath


MRRSet = Dict[Tuple[UUID, str], MetadataRootRecord]


def create_mrr(uuid: UUID, version: str) -> MetadataRootRecord:
    return MetadataRootRecord(
        dataset_identifier=uuid,
        dataset_version=version,
        dataset_level_metadata=create_metadata(
            f"dataset-level metadata for {uuid}@{version}"
        ),
        file_tree=create_file_tree()
    )


def create_metadata_root_records(descriptions: List[DatasetDescription]
                                 ) -> MRRSet:
    return {
        (description.uuid, description.version): MetadataRootRecord(
            dataset_identifier=description.uuid,
            dataset_version=description.version,
            dataset_level_metadata=create_metadata(
                f"dataset-level metadata for "
                f"{description.uuid}@{description.version}"
            ),
            file_tree=create_file_tree()
        )
        for description in descriptions
    }


def create_metadata(metadata_content: str) -> Metadata:
    metadata = Metadata()
    metadata.add_extractor_run(
        time_stamp=None,
        extractor_name="fake_extractor",
        author_name="Fake Author",
        author_email="fake.author@example.com",
        configuration=ExtractorConfiguration("v1.0.0", {"p1": "v1"}),
        metadata_content={"metadata_content": metadata_content}
    )
    return metadata


def create_file_tree():
    file_tree = FileTree()
    for metadata_path in [MetadataPath(f) for f in ["file_1", "file_2"]]:
        file_tree.add_metadata(
            metadata_path,
            create_metadata(str(metadata_path))
        )
    return file_tree


def create_dataset_tree(descriptions: List[DatasetDescription],
                        mrr_set: MRRSet
                        ) -> DatasetTree:
    """create a dataset tree with the given descriptions and mrr set"""
    dataset_tree = DatasetTree()
    for description in descriptions:
        dataset_tree.add_dataset(
            description.path,
            mrr_set[(description.uuid, description.version)]
        )
    return dataset_tree


def create_uuid_set(tree_version_list: TreeVersionList) -> UUIDSet:

    instances = {}
    for version, (time_stamp, path, dataset_tree) in tree_version_list.versioned_elements:
        dataset_tree = cast(DatasetTree, dataset_tree)
        for dataset_path, mrr in dataset_tree.dataset_paths:
            if mrr.dataset_identifier not in instances:
                instances[mrr.dataset_identifier] = dict()
            instances[mrr.dataset_identifier][mrr.dataset_version] = mrr

    uuid_set = UUIDSet()
    for uuid, mrr_dict in instances.items():
        version_list = VersionList()
        for version, mrr in mrr_dict.items():
            version_list.set_versioned_element(
                version,
                str(time.time()),
                MetadataPath(""),
                mrr
            )
        uuid_set.set_version_list(uuid, version_list)
    return uuid_set


def main():

    path = sys.argv[1]

    descriptions = [
        DatasetDescription(
            UUID("00010203-1011-2021-3031-404142434400"),
            "0000000000000000000000000000000000000000",
            MetadataPath("")
        ),
        DatasetDescription(
            UUID("01010203-1011-2021-3031-404142434401"),
            "0000000000000000000000000000000000000001",
            MetadataPath("dataset1")
        ),
        DatasetDescription(
            UUID("02010203-1011-2021-3031-404142434402"),
            "0000000000000000000000000000000000000002",
            MetadataPath("dataset2")
        ),
        DatasetDescription(
            UUID("03010203-1011-2021-3031-404142434403"),
            "0000000000000000000000000000000000000003",
            MetadataPath("dataset3")
        ),
    ]

    # Add a first version
    metadata_root_records_1 = create_metadata_root_records(descriptions)
    dataset_tree_1 = create_dataset_tree(descriptions, metadata_root_records_1)

    tree_version_list = TreeVersionList(realm=path)
    tree_version_list.set_dataset_tree(
        descriptions[0].version,
        str(time.time()),
        dataset_tree_1
    )

    # Add a second version
    descriptions[0] = DatasetDescription(
        UUID("00010203-1011-2021-3031-404142434400"),
        "0000000000000000000000000000000000000010",
        MetadataPath("")
    )

    metadata_root_records_2 = create_metadata_root_records(descriptions)
    dataset_tree_2 = create_dataset_tree(descriptions, metadata_root_records_2)
    tree_version_list.set_dataset_tree(
        descriptions[0].version,
        str(time.time()),
        dataset_tree_2
    )

    uuid_set = create_uuid_set(tree_version_list)

    tree_version_list.write_out(destination_realm=path)
    uuid_set.write_out(destination_realm=path)


if __name__ == "__main__":
    main()
