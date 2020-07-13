from abc import ABC, abstractmethod
from itertools import count
from typing import Optional


class LocationInfo(object):
    pass


class FileLocation(LocationInfo):
    def __init__(self, file_name: str, line: int, column: int):
        self.file_name = file_name
        self.line = line
        self.column = column

    def __repr__(self):
        return f"CodeLocation({self.file_name}, {self.line}, {self.column})"

    def __str__(self):
        return f"{self.file_name}:{self.line}:{self.column}"


class ObjectLocation(LocationInfo):
    def __init__(self, file_name: str, dotted_name: str, locations: dict):
        self.file_name = file_name
        self.dotted_name = dotted_name
        self.line = locations[dotted_name].line
        self.column = locations[dotted_name].column

    def __repr__(self):
        return f"ObjectLocation({repr(self.file_name)}, {self.dotted_name})"

    def __str__(self):
        return f"{self.file_name}:{self.line + 1}:{self.column + 1}"


class StringLocation(LocationInfo):
    def __init__(self, location: str):
        self.location = location

    def __repr__(self):
        return f"StringLocation({self.location})"

    def __str__(self):
        return f"{self.location}"


class ValidatorMessage(ABC):
    def __init__(self, text: str, location: LocationInfo, indent_output: Optional[bool] = True):
        self.text = text
        self.location = location
        self.indent_output = indent_output

    def __repr__(self):
        return f"ValidatorMessage({repr(self.text)}, {repr(self.location)}, {repr(self.indent_output)})"

    def __str__(self):
        context = f"{self.location}: {self.level_description()}"
        if self.indent_output:
            lines = [f"{context}: {line}" if index == 0 else f"{' ' * len(context)}  {line}"
                     for index, line in enumerate(self.text.splitlines())]
        else:
            lines = [f"{context}: {line}" if index == 0 else f"{line}"
                     for index, line in enumerate(self.text.splitlines())]
        return "\n".join(lines) + "\n"

    @abstractmethod
    def error_score(self) -> int:
        pass

    @abstractmethod
    def level_description(self) -> int:
        pass


class ErrorMessage(ValidatorMessage):
    def __repr__(self):
        return f"ErrorMessage({repr(self.text)}, {repr(self.location)})"

    def error_score(self) -> int:
        return 1

    def level_description(self) -> str:
        return "error"


class WarningMessage(ValidatorMessage):
    def __repr__(self):
        return f"WarningMessage({repr(self.text)}, {repr(self.location)})"

    def error_score(self) -> int:
        return 0

    def level_description(self) -> str:
        return "warning"
