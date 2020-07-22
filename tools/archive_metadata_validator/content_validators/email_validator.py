from typing import List

from content_validators.content_validator import ContentValidator
from messages import ObjectLocation, ValidatorMessage, WarningMessage


class EmailValidator(ContentValidator):

    def perform_validation(self) -> List[ValidatorMessage]:
        messages = []
        for path, _ in self.persons():
            if path[-1].endswith("@example.com"):
                dotted_name = ContentValidator.unescape_name(ContentValidator.path_to_dotted_name(path))
                messages += [
                    WarningMessage(
                        f"dummy email address found: '{path[-1]}'",
                        ObjectLocation(self.file_name, dotted_name, self.object_locations))]
        return messages
