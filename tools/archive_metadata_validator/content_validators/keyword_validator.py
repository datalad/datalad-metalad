from typing import List

from messages import ValidatorMessage, WarningMessage, StringLocation
from .content_validator import ContentValidator


KEYWORD_LOOKUP_URL = "https://jugit.fz-juelich.de/inm7/datasets/datasets_repo/-/wikis/Dataset-Keywords"


class KeywordValidator(ContentValidator):
    def perform_validation(self) -> List[ValidatorMessage]:
        context = "dataset.keyword"
        keywords = self.value_at(context)
        if not keywords:
            return [
                WarningMessage(
                    f"{context}: no keywords given for dataset, please consider adding some "
                    f"(see <{KEYWORD_LOOKUP_URL}> for a list of possible keywords)",
                    StringLocation(self.file_name))]
        return []
