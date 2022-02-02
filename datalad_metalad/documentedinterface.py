import dataclasses
from typing import (
    Dict,
    List,
    Optional,
)

from datalad.support.constraints import Constraints


@dataclasses.dataclass(frozen=True)
class ParameterEntry:
    keyword: str
    help: str
    optional: bool = False
    constraints: List[Constraints] = None


class DocumentedInterface:

    def __init__(self,
                 name: str,
                 help_entries: List[ParameterEntry],
                 constraints: Optional[Constraints] = None):

        self.name = name
        self.help_entries = help_entries
        self.constraints = constraints

        self.required_entries, self.optional_entries = [
            [
                entry
                for entry in help_entries
                if entry.optional is condition
            ]
            for condition in (False, True)
        ]
        self.all_keys = set([entry.keyword for entry in help_entries])

    def check_keys_values(self,
                          keys_values: Dict[str, str]) -> List:

        missing_keys = [
            (f"required key: '{self.name}:{entry.keyword}' missing", entry.help)
            for entry in self.required_entries
            if entry.keyword not in keys_values
        ]

        unknown_keys = [
            (f"unknown key: '{self.name}:{key}'", "")
            for key in keys_values
            if key not in self.all_keys
        ]
        return missing_keys + unknown_keys
