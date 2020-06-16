import re
from functools import reduce
from typing import List, Union

from .content_validator import ContentValidator


ORCID_ID_PREFIX = "https://orcid.org/"

ORCID_ID_REGEX_PATTERN = "^([0-9]{4}-){3}[0-9]{3}[0-9X]$"


class ORCIDIDValidator(ContentValidator):
    @staticmethod
    def _check_digit(base_digits: str):
        total = reduce(lambda x, y: (x + y) * 2, map(int, base_digits), 0)
        result = (12 - (total % 11)) % 11
        return str(result) if result < 10 else "X"

    def _get_orcidid_for_person(self, person: dict) -> Union[str, None]:
        orcid_id = self.value_at("orcid-id", person)
        if orcid_id is not None:
            orcid_id = orcid_id.strip()
            if orcid_id.lower().startswith(ORCID_ID_PREFIX):
                orcid_id = orcid_id[len(ORCID_ID_PREFIX):].strip()
        return orcid_id

    def _check_orcidid(self, orcid_id: str, email: str) -> List:
        if len(orcid_id) != 19:
            return [f"ORCID-ID error: ORCID-ID too short ('{orcid_id}') for person with email: {email}"]
        if re.match(ORCID_ID_REGEX_PATTERN, orcid_id) is None:
            return [f"ORCID-ID error: ORCID-ID invalid format ('{orcid_id}') for person with email: {email}"]
        orcid_id_digits = orcid_id.replace("-", "")
        if self._check_digit(orcid_id_digits[:15]) != orcid_id_digits[15]:
            return [f"ORCID-ID error: ORCID-ID invalid checksum ('{orcid_id}') for person with email: {email}"]
        return []

    def perform_validation(self, spec: dict) -> List:
        errors = []
        for (email, person_spec) in self.value_at("person", spec, default={}).items():
            orcid_id = self._get_orcidid_for_person(person_spec)
            if orcid_id is not None:
                errors += self._check_orcidid(orcid_id, email)
        return errors
