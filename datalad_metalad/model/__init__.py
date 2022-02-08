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
