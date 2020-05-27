from collections import namedtuple
from typing import List

from .content_validator import ContentValidator


PersonInfo = namedtuple("PersonInfo", ["first_name", "last_name", "id"])


class ReferenceValidator(ContentValidator):
    def _validate_person_reference(self, person_ref) -> List:
        if isinstance(person_ref, str):
            result = any(map(lambda person: person.id == person_ref, self.persons))
        else:
            result = any(map(
                lambda person: (
                    person.last_name == person_ref["last_name"] and
                    person.first_name == person_ref["first_name"]
                ), self.persons))
        if result is False:
            return [f"Reference error: faulty person reference ({person_ref})"]
        return []

    def _validate_nullable_person_reference(self, person_ref) -> List:
        if person_ref is not None:
            return self._validate_person_reference(person_ref)
        return []

    def _get_person_info(self, spec: dict):
        self.persons = tuple((
            PersonInfo(
                self.value_at("person.first_name", entry),
                self.value_at("person.last_name", entry),
                self.value_at("person.id", entry)
            ) for entry in self.value_at("study.persons", spec, default=[])
        ))

    def perform_validation(self, spec: dict) -> List:
        errors = []
        self._get_person_info(spec)
        for publication_spec in self.value_at("study.publications", spec, default=[]):
            for person_ref in self.value_at("publication.authors", publication_spec):
                errors += self._validate_person_reference(person_ref)
        errors += self._validate_nullable_person_reference(self.value_at("study.contact_point", spec))
        errors += self._validate_nullable_person_reference(self.value_at("study.dataset.contact_point", spec))
        return errors
