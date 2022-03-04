"""
The following datatypes are used by metalad

    # Described in datalad result_record documentation
    Result:
        # Status is one of: ok, notneeded, impossible, error.
        # Error cases are: "impossible", and "error"
        status: str
        # Absolute platform-specific path of the result.
        path: str
        action: str

        # Optional

        # "file" or "dataset"
        type: str


    CoreMetadataInfo:

        # Extractor setting
        filter_name: str
        extractor_version: str
        extraction_parameter: Dictionary

        # Extraction process info
        extraction_time: float
        agent_name: str
        agent_email: str

        # Enclosing dataset and metadata location
        dataset_id: UUID
        dataset_version: str
        path: MetadataPath = MetadataPath(".")  (can only be None or MetadataPath(".") if type is 'dataset')

        # Optional dataset tree information, which is used for aggregation.
        # It contains a root dataset id, a root dataset version and the path
        # of the containing dataset within the root dataset tree.
        # The set of keys should be present or non-present in its entirety.
        root_dataset_id: UUID = None
        root_dataset_version: str = None
        dataset_path: MetadataPath = None

        type: str ('file' or 'dataset')
        extracted_metadata: JSONType

    MetadataContext:





"""
from typing import (
    Any,
    Dict,
    List,
    Union
)

_JSONType_0 = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]
_JSONType_1 = Union[str, int, float, bool, None, Dict[str, _JSONType_0], List[_JSONType_0]]
_JSONType_2 = Union[str, int, float, bool, None, Dict[str, _JSONType_1], List[_JSONType_1]]
_JSONType_3 = Union[str, int, float, bool, None, Dict[str, _JSONType_2], List[_JSONType_2]]

JSONType = Union[str, int, float, bool, None, Dict[str, _JSONType_3], List[_JSONType_3]]


