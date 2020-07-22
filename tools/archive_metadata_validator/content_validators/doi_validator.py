import requests
from typing import List, Union

import json

from messages import ValidatorMessage, ErrorMessage, WarningMessage, ObjectLocation
from .content_validator import ContentValidator


DOI_RESOLVER_BASE_URL = "https://doi.org/api/handles/"
DOI_PREFIXES = ("doi:", "doi: ", "https://doi.org/")


class DOIValidator(ContentValidator):
    @staticmethod
    def _doi_is_resolvable(doi_str: str) -> bool:
        result = requests.get(DOI_RESOLVER_BASE_URL + doi_str)
        response_object = json.loads(result.text)
        response_code = response_object["responseCode"]
        if response_code in (1, 2):
            return True
        return False

    def _get_doi_for_publication(self, publication: dict) -> Union[str, None]:
        doi_str = self.value_at_in_spec("doi", publication)
        if doi_str is not None:
            doi_str = doi_str.strip()
            for prefix in DOI_PREFIXES:
                if doi_str.lower().startswith(prefix):
                    doi_str = doi_str[len(prefix):].strip()
                    break
        return doi_str

    def perform_validation(self) -> List[ValidatorMessage]:
        messages = []
        seen_doi_str = []
        for index, publication_spec in enumerate(self.value_at("publication", default=[])):
            doi_str = self._get_doi_for_publication(publication_spec)
            if doi_str:
                dotted_name = ContentValidator.path_to_dotted_name(["publication", index, "doi"])
                if doi_str in seen_doi_str:
                    messages.append(
                        WarningMessage(
                            f"identical doi ({doi_str}) already used in other publication",
                            ObjectLocation(self.file_name, dotted_name, self.object_locations)))
                if self._doi_is_resolvable(doi_str) is False:
                    messages.append(
                        ErrorMessage(
                            f"DOI unresolvable ({doi_str})",
                            ObjectLocation(self.file_name, dotted_name, self.object_locations)))
                seen_doi_str.append(doi_str)
        return messages
