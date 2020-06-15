import requests
from typing import List, Union

import json

from .content_validator import ContentValidator


DOI_RESOLVER_BASE_URL = "https://doi.org/api/handles/"


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
        doi_str = self.value_at("doi", publication)
        if doi_str is not None:
            doi_str = doi_str.strip()
            if doi_str.lower().startswith("doi:"):
                doi_str = doi_str[4:].strip()
        return doi_str

    def perform_validation(self, spec: dict) -> List:
        errors = []
        for publication_spec in self.value_at("publication", spec, default=[]):
            doi_str = self._get_doi_for_publication(publication_spec)
            if doi_str and self._doi_is_resolvable(doi_str) is False:
                errors += [f"DOI error: unresolvable DOI ('{doi_str}') in "
                           f'publication with title: "{publication_spec["title"]}"']
        return errors
