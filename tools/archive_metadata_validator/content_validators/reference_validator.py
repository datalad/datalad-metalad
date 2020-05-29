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
        self.ids = tuple((person_info.id for person_info in self.persons if person_info.id is not None))

    def _check_id_uniqueness(self) -> List:
        """ check whether person ids are unique """
        if len(set(self.ids)) != len(self.ids):
            repeated_ids = set(filter(lambda x: self.ids.count(x) > 1, self.ids))
            return [f"Reference error: repeated ids: {','.join((i for i in repeated_ids))}"]
        return []

    def _check_name_uniqueness(self) -> List:
        """ check whether first name, last name of persons without ids are unique """
        name_identified_persons = tuple((
            (person.first_name, person.last_name)
            for person in filter(lambda person_info: person_info.id is None, self.persons)
        ))

        if len(set(name_identified_persons)) != len(name_identified_persons):
            repeated_names = set(filter(lambda x: name_identified_persons.count(x) > 1, name_identified_persons))
            return [f"Reference error: repeated names without id: "
                    f"{', '.join((f'``{i[0]} {i[1]}´´' for i in set(repeated_names)))}"]
        return []
        pass

    def perform_validation(self, spec: dict) -> List:
        self._get_person_info(spec)
        errors = self._check_id_uniqueness()
        errors += self._check_name_uniqueness()
        for publication_spec in self.value_at("study.publications", spec, default=[]):
            for person_ref in self.value_at("publication.authors", publication_spec):
                errors += self._validate_person_reference(person_ref)
        errors += self._validate_nullable_person_reference(self.value_at("study.contact_person", spec))
        errors += self._validate_nullable_person_reference(self.value_at("study.dataset.contact_person", spec))
        return errors
