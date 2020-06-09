from collections import namedtuple
from typing import List

from .content_validator import ContentValidator


PersonInfo = namedtuple("PersonInfo", ["first_name", "last_name", "id"])


class ReferenceValidator(ContentValidator):
    @staticmethod
    def _validate_person_reference(person_ref: str, spec: dict) -> List:
        if person_ref not in spec["person"]:
            return [f"Reference error: reference to undefined person ({person_ref})"]
        return []

    def _validate_nullable_person_reference(self, person_ref, spec: dict) -> List:
        if person_ref is not None:
            return self._validate_person_reference(person_ref, spec)
        return []

    def perform_validation(self, spec: dict) -> List:
        errors = []
        for publication_spec in self.value_at("publication", spec, default=[]):
            author = self.value_at("author", publication_spec)
            for person_ref in [author] if isinstance(author, str) else author:
                errors += self._validate_person_reference(person_ref, spec)
            corresponding_author_ref = self.value_at("corresponding_author", publication_spec)
            if corresponding_author_ref is not None:
                if corresponding_author_ref not in publication_spec["author"]:
                    errors += [f"Reference error: corresponding_author not in author ({corresponding_author_ref})"]
        errors += self._validate_person_reference(self.value_at("study.principal_investigator", spec), spec)
        for person_ref in self.value_at("study.contributor", spec, default=[]):
            errors += self._validate_person_reference(person_ref, spec)
        for person_ref in self.value_at("dataset.author", spec, default=[]):
            errors += self._validate_person_reference(person_ref, spec)
        return errors
