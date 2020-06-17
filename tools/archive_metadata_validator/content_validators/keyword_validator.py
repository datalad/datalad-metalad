from typing import List

from messages import ValidatorMessage, ValidatorMessageSeverity
from .content_validator import ContentValidator


KEYWORD_LOOKUP_URL = "https://jugit.fz-juelich.de/c.moench/datasets_repo/-/wikis/Dataset-Keywords"


class KeywordValidator(ContentValidator):
    def perform_validation(self, spec: dict) -> List[ValidatorMessage]:
        keywords = self.value_at("dataset.keyword", spec)
        if not keywords:
            return [ValidatorMessage("Warning: no keywords given for dataset, please consider adding some. "
                                     f"See <{KEYWORD_LOOKUP_URL}> for a list of possible keywords.",
                                     ValidatorMessageSeverity.WARNING)]
        return []
