import sys
from enum import Enum
from typing import Optional


class ValidatorMessageSeverity(Enum):
    ERROR = 1
    WARNING = 2


class ValidatorMessage(object):
    def __init__(self, text: str, severity: Optional[ValidatorMessageSeverity] = 1):
        self.text = text
        self.severity = severity

    def write_to(self, stream, header: str):
        for index, line in enumerate(self.text.splitlines()):
            if index == 0:
                stream.write(f"{header}: {line}\n")
            else:
                stream.write(f"{' ' * len(header)}  {line}\n")
