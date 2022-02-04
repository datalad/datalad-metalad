import dataclasses
import enum
from pathlib import Path
from typing import (
    Dict,
    Optional,
)
from uuid import UUID

from . import JSONType
from dataladmetadatamodel.metadatapath import MetadataPath


@dataclasses.dataclass
class ExtractionParameter:
    version: str
    parameter: Dict[str, str]


class MetadataType(enum.Enum):
    File = "file"
    Dataset = "dataset"

    def __str__(self):
        return self.value


@dataclasses.dataclass
class MetadataContext:
    """Dataclass to represent metadata, including provenance and referee

    Instances of this class contain metadata and information about the source
    of the metadata, i.e. the extraction process and the extractor agent. They
    also contain information about the element that this metadata is associated
    with, i.e. a dataset and a path within the dataset.
    """
    type: MetadataType
    extractor_name: str
    extractor_version: str
    extraction_parameter: Dict[str, str]
    extraction_time: float
    agent_name: str
    agent_email: str
    dataset_id: UUID
    dataset_version: str
    extracted_metadata: JSONType

    path: Optional[MetadataPath] = None

    root_dataset_id: Optional[UUID] = None
    root_dataset_version: Optional[str] = None
    dataset_path: Optional[Path] = None

    def __post_init__(self):
        self._check_type()
        self._check_aggregation_keys()

    def _check_type(self):
        if self.type not in (MetadataType.File, MetadataType.Dataset):
            raise ValueError(
                f"illegal metadata type: '{type}' "
                f"(must be one of 'file', 'dataset')")
        if self.type == MetadataType.File and self.path is None:
            raise ValueError(f"Missing path in 'file'-type metadata object")

    def _check_aggregation_keys(self):
        aggregation_keys = [
            ("root_dataset_id", self.root_dataset_id),
            ("root_dataset_version", self.root_dataset_version),
            ("dataset_path", self.dataset_path)
        ]

        if any(x[1] is not None for x in aggregation_keys):

            unset_aggregation_keys = (
                f"'{x[0]}'" for x in aggregation_keys if x[1] is None)

            if unset_aggregation_keys:
                raise ValueError(
                    "Missing aggregation keys: "
                    f"{', '.join(unset_aggregation_keys)}")
