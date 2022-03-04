import json
from dataclasses import (
    dataclass,
    asdict,
)
from pathlib import Path
from typing import (
    Dict,
    List,
    Optional,
    Union,
    cast,
)
from uuid import UUID

from dataladmetadatamodel.metadatapath import MetadataPath

from . import JSONType
from .result import (
    Result,
    ACTION,
    PATH,
    STATUS,
)


DATASET = "dataset"
FILE = "file"

DATASET_ID = "dataset_id"
TYPE = "type"
EXTRACTION_PARAMETER = "extraction_parameter"

DATASET_PATH = "dataset_path"
ROOT_DATASET_ID = "root_dataset_id"
ROOT_DATASET_VERSION = "root_dataset_version"

METADATA_RECORD = "metadata_record"
METADATA_SOURCE = "metadata_source"
BACKEND = "backend"
META_FILTER = "meta_filter"


#@dataclass(eq=True, frozen=True)
#class ExtractionParameter:
#    args: List
#    kwargs: Dict


@dataclass(eq=True, frozen=True)
class Metadata:
    type: str
    extractor_name: str
    extractor_version: str
    extraction_parameter: Dict[str, JSONType]
    extraction_time: float
    agent_name: str
    agent_email: str
    dataset_id: UUID
    dataset_version: str
    extracted_metadata: JSONType
    path: MetadataPath = MetadataPath(".")
    root_dataset_id: Optional[UUID] = None
    root_dataset_version: Optional[str] = None
    dataset_path: Optional[MetadataPath] = None

    def __post_init__(self):
        if self.type not in (DATASET, FILE):
            raise ValueError(f"unknown type: '{self.type}'")
        if self.path == MetadataPath(".") and self.type != DATASET:
            raise ValueError(f"root-path in type: '{self.type}'")

    def as_json_obj(self) -> JSONType:
        result = {
            **asdict(self),
            **{
                DATASET_ID: str(self.dataset_id),
                PATH: str(self.path),
            },
            **(
                {
                    ROOT_DATASET_ID: (
                        str(self.root_dataset_id)
                        if self.root_dataset_id is not None
                        else None
                    ),
                    ROOT_DATASET_VERSION: self.root_dataset_version,
                    DATASET_PATH: (
                        str(self.dataset_path)
                        if self.dataset_path is not None
                        else None
                    )
                }
            )
        }
        if result[ROOT_DATASET_ID] is None:
            del result[ROOT_DATASET_ID]
            del result[ROOT_DATASET_VERSION]
            del result[DATASET_PATH]
        return result

    def as_json_str(self):
        return json.dumps(self.as_json_obj())

    @classmethod
    def from_json(cls, json_obj):
        return cls(**{
            **json_obj,
            **{
                DATASET_ID: UUID(json_obj[DATASET_ID]),
                PATH: MetadataPath(json_obj.get(PATH, "")),
            },
            **{
                ROOT_DATASET_ID: (
                    UUID(json_obj[ROOT_DATASET_ID])
                    if json_obj.get(ROOT_DATASET_ID, None)
                    else None
                ),
                ROOT_DATASET_VERSION: json_obj.get(ROOT_DATASET_VERSION, None),
                DATASET_PATH: (
                    MetadataPath(json_obj[DATASET_PATH])
                    if json_obj.get(DATASET_PATH, None)
                    else None
                )
            }
        })


class MetadataResult(Result):
    def __init__(self,
                 status: str,
                 path: Union[str, Path],
                 action: str,
                 metadata_type: str,
                 metadata_record: Metadata,
                 metadata_source: Union[str, Path],
                 backend: str):

        Result.__init__(self, status, path, action)
        self.metadata_type = cast(str, metadata_type)
        self.metadata_record = metadata_record
        self.metadata_source = Path(metadata_source)
        self.backend = cast(str, backend)

        if self.metadata_type not in (FILE, DATASET):
            raise ValueError(f"Unknown metadata type: {self.metadata_type}")

    def __repr__(self):
        return f"MetadataResult({self.status}, {self.path}, {self.action}, " \
               f"{self.metadata_type}, {self.metadata_source}, {self.backend})"

    def as_json_obj(self) -> JSONType:
        return {
            **Result.as_json_obj(self),
            **{
                TYPE: self.metadata_type,
                METADATA_RECORD: self.metadata_record.as_json_obj(),
                METADATA_SOURCE: self.metadata_source,
                BACKEND: self.backend
            }
        }

    @staticmethod
    def from_json_obj(json_obj) -> "MetadataResult":
        return MetadataResult(
            json_obj[STATUS],
            json_obj[PATH],
            json_obj[ACTION],
            json_obj[TYPE],
            Metadata.from_json(json_obj[METADATA_RECORD]),
            json_obj[METADATA_SOURCE],
            json_obj[BACKEND])
