from pathlib import Path
from typing import (
    Tuple,
    Union,
)

from dataladmetadatamodel.common import get_top_level_metadata_objects
from dataladmetadatamodel.mapper.reference import Reference

from .exceptions import NoMetadataStoreFound


def get_metadata_objects(dataset: Union[str, Path],
                         backend: str,
                         ) -> Tuple:

    # Initialize the metadata coordinates
    metadata_store_path = dataset \
        if Reference.is_remote(str(dataset or ".")) \
        else Path(dataset or ".")

    tree_version_list, uuid_set = get_top_level_metadata_objects(
        backend,
        metadata_store_path)

    # We require both entry points to exist for valid metadata
    if tree_version_list is None or uuid_set is None:
        raise NoMetadataStoreFound(
            f"No valid datalad metadata found in: "
            f"{Path(metadata_store_path).resolve()}")

    return metadata_store_path, tree_version_list, uuid_set
