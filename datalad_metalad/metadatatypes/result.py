import json
from pathlib import Path
from typing import (
    Union,
    cast,
)

from . import JSONType

STATUS = "status"
ACTION = "action"
PATH = "path"

OK = "ok"
NOTNEEDED = "notneeded"
IMPOSSIBLE = "impossible"
ERROR = "error"

TYPE = "type"
FILE = "file"
DATASET = "dataset"


class Result:
    def __init__(self,
                 status: str,
                 path: Union[str, Path],
                 action: str):

        self.status = cast(str, status)
        self.path = Path(path)
        self.action = cast(str, action)

        if self.status not in (OK, NOTNEEDED, IMPOSSIBLE, ERROR):
            raise ValueError(f"Unknown status: {self.status}")
        if not self.path.is_absolute():
            raise ValueError(f"Non-absolute path: {self.path}")

    def __repr__(self):
        return f"Result({self.status}, {self.path}, {self.action})"

    def as_json_obj(self) -> JSONType:
        return {
            STATUS: self.status,
            ACTION: self.action,
            PATH: str(self.path)
        }

    @staticmethod
    def from_json_obj(json_obj) -> "Result":
        return Result(
            json_obj[STATUS],
            json_obj[PATH],
            json_obj[ACTION])

    @classmethod
    def from_json_str(cls, json_str) -> "Result":
        return cls.from_json_obj(json.loads(json_str))
