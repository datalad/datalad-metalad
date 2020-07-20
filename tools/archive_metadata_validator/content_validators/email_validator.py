from typing import List

from content_validators.content_validator import ContentValidator
from messages import ObjectLocation, ValidatorMessage, WarningMessage


class EmailValidator(ContentValidator):

    def perform_validation(self) -> List[ValidatorMessage]:
        messages = []
        for path, _ in self.persons():
            if path[-1].endswith("@example.com"):
                messages += [
                    WarningMessage(
                        f"dummy email address found: '{path}'",
                        ObjectLocation(self.file_name, path, self.object_locations))]
        return messages
