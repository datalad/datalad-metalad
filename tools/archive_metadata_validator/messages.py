from abc import ABC, abstractmethod
from typing import Optional


class FileLocation(object):
    def __init__(self, file_name: str, line: int, column: int):
        self.file_name = file_name
        self.line = line
        self.column = column

    def __repr__(self):
        return f"FileLocation({repr(self.file_name)}, {self.line}, {self.column})"

    def __str__(self):
        return f"{self.file_name}:{self.line if self.line >= 0 else '?'}:{self.column if self.column >= 0 else '?'}"


class ObjectLocation(FileLocation):
    def __init__(self, file_name: str, dotted_name: str, locations: dict):
        self.dotted_name = dotted_name
        self.locations = locations
        if locations and dotted_name in locations:
            line = locations[dotted_name].line + 1
            column = locations[dotted_name].column + 1
        else:
            line = 0
            column = 0
        super(ObjectLocation, self).__init__(file_name, line, column)

    def __repr__(self):
        return f"ObjectLocation({repr(self.file_name)}, {repr(self.dotted_name)}, {repr(self.locations)})"


class ValidatorMessage(ABC):
    def __init__(self, text: str, location: FileLocation, indent_output: Optional[bool] = True):
        self.text = text
        self.location = location
        self.indent_output = indent_output

    def __repr__(self):
        return f"{self._class_repr()}({repr(self.text)}, {repr(self.location)}, {repr(self.indent_output)})"

    def __str__(self):
        context = f"{self.location}: {self.level_description()}"
        if self.indent_output:
            lines = [f"{context}: {line}" if index == 0 else f"{' ' * len(context)}  {line}"
                     for index, line in enumerate(self.text.splitlines())]
        else:
            lines = [f"{context}: {line}" if index == 0 else f"{line}"
                     for index, line in enumerate(self.text.splitlines())]
        return "\n".join(lines) + "\n"

    def _class_repr(self) -> str:
        return "ValidatorMessage"

    @abstractmethod
    def error_score(self) -> int:
        pass

    @abstractmethod
    def level_description(self) -> int:
        pass


class ErrorMessage(ValidatorMessage):
    def _class_repr(self) -> str:
        return "ErrorMessage"

    def error_score(self) -> int:
        return 1

    def level_description(self) -> str:
        return "error"


class WarningMessage(ValidatorMessage):
    def _class_repr(self) -> str:
        return "WarningMessage"

    def error_score(self) -> int:
        return 0

    def level_description(self) -> str:
        return "warning"
