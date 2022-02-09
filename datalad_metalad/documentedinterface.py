import dataclasses
import textwrap
from typing import (
    Callable,
    Dict,
    List,
)


@dataclasses.dataclass(frozen=True)
class ParameterEntry:
    keyword: str
    help: str
    optional: bool = False
    constraints: Callable = None


class DocumentedInterface:

    def __init__(self,
                 description: str,
                 parameter_entries: List[ParameterEntry]):

        self.description = description
        self.parameter_entries = parameter_entries

        self.parameter_entry_by_name = {
            entry.keyword: entry
            for entry in parameter_entries
        }

        if len(self.parameter_entry_by_name.keys()) != len(parameter_entries):
            raise ValueError("duplicated parameter name")

        self.required_entries, self.optional_entries = [
            [
                entry
                for entry in parameter_entries
                if entry.optional is condition
            ]
            for condition in (False, True)
        ]
        self.all_keys = set([entry.keyword for entry in parameter_entries])

    def check_keys_values(self,
                          name: str,
                          key_value_dict: Dict[str, str]
                          ) -> List:
        missing_keys = self._get_missing_keys(name, key_value_dict)
        unknown_keys = self._get_unknown_keys(name, key_value_dict)
        return missing_keys + unknown_keys

    def get_description(self,
                        name: str) -> str:
        return f"{name}:" + "\n".join(textwrap.wrap(self.description))

    def get_entry_description(self,
                              name: str) -> str:
        return "\n".join(
            self._render_entry(name, entry)
            for entry in self.parameter_entries)

    def _render_entry(self,
                      name: str,
                      entry: ParameterEntry):
        if entry.constraints:
            constraints_text = "Allowed values: " + str(entry.constraints)
        else:
            constraints_text = ""

        help_text = "\n".join(textwrap.wrap(entry.help)) + "\n"
        return "{name}.{keyword} {optional}\n" \
               "{description}\n" \
               "{values}".format(
                    name=name,
                    keyword=entry.keyword,
                    optional="  (optional)" if entry.optional is True else "",
                    description=help_text,
                    values=constraints_text)

    def _get_missing_keys(self,
                          name: str,
                          key_value_dict: Dict[str, str]
                          ) -> List:
        return [
            (f"required key: '{name}:{entry.keyword}' missing", entry.help)
            for entry in self.required_entries
            if entry.keyword not in key_value_dict]

    def _get_unknown_keys(self,
                          name: str,
                          key_value_dict: Dict[str, str]
                          ) -> List:
        return [
            (f"unknown key: '{name}:{key}'", "")
            for key in key_value_dict
            if key not in self.all_keys]

    def _check_constraints(self,
                           name: str
                           ) -> List:
        return []
