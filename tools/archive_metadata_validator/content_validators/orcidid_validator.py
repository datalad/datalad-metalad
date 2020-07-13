import re
from functools import reduce
from typing import List, Tuple, Union

from messages import ValidatorMessage, ErrorMessage, WarningMessage, ObjectLocation, StringLocation
from .content_validator import ContentValidator


ORCID_ID_PREFIX = "https://orcid.org/"

ORCID_ID_REGEX_PATTERN = "^([0-9]{4}-){3}[0-9]{3}[0-9X]$"


class ORCIDIDValidator(ContentValidator):
    @staticmethod
    def _check_digit(base_digits: str):
        total = reduce(lambda x, y: (x + y) * 2, map(int, base_digits), 0)
        result = (12 - (total % 11)) % 11
        return str(result) if result < 10 else "X"

    def _get_orcidid_for_person(self, person: dict) -> Tuple[Union[str, None], bool]:
        orcid_id = self.value_at("orcid-id", person)
        if orcid_id is not None:
            orcid_id = orcid_id.strip()
            if orcid_id.lower().startswith(ORCID_ID_PREFIX):
                orcid_id = orcid_id[len(ORCID_ID_PREFIX):].strip()
                return orcid_id, True
        return orcid_id, False

    def _check_orcidid(self, orcid_id: str, email: str) -> List[ValidatorMessage]:
        location = StringLocation(f"{self.file_name}:person with email: {email}")
        if re.match(ORCID_ID_REGEX_PATTERN, orcid_id) is None:
            return [
                ErrorMessage(
                    f"ORCID-ID invalid ({orcid_id}), format is not XXXX-XXXX-XXXX-XXXX", location)]

        orcid_id_digits = orcid_id.replace("-", "")
        if self._check_digit(orcid_id_digits[:15]) != orcid_id_digits[15]:
            return [
                ErrorMessage(
                    f"ORCID-ID invalid ({orcid_id}), checksum failed", location)]
        return []

    def perform_validation(self, spec: dict) -> List[ValidatorMessage]:
        messages = []
        seen_orcid_ids = []
        for (email, person_spec) in self.value_at("person", spec, default={}).items():
            orcid_id, has_prefix = self._get_orcidid_for_person(person_spec)
            if orcid_id is not None:
                dotted_name = f"person.{email}.orcid-id"
                location = ObjectLocation(self.file_name, dotted_name, self.source_positions)
                if orcid_id in seen_orcid_ids:
                    messages.append(
                        WarningMessage(
                            f"duplicated ORCID-ID ({orcid_id}) in {dotted_name}", location))
                else:
                    seen_orcid_ids.append(orcid_id)
                if has_prefix is False:
                    messages.append(
                        WarningMessage(
                            f"ORCID-ID is missing prefix ({ORCID_ID_PREFIX}) in {dotted_name}", location))
                messages += self._check_orcidid(orcid_id, email)
        return messages
