from collections import namedtuple
from typing import Iterable, List

from .content_validator import ContentValidator


PersonInfo = namedtuple("PersonInfo", ["first_name", "last_name", "id"])


class ReferenceValidator(ContentValidator):
    @staticmethod
    def _validate_person_reference(person_ref: str, spec: dict, context="") -> List:
        if person_ref not in spec["person"]:
            if context:
                return [f"Reference error: reference to undefined person ({person_ref}) in {context}"]
            else:
                return [f"Reference error: reference to undefined person ({person_ref})"]
        return []

    def _validate_nullable_person_reference(self, person_ref, spec: dict, context="") -> List:
        if person_ref is not None:
            return self._validate_person_reference(person_ref, spec, context)
        return []

    def _person_ref_list(self, dotted_name: str, spec: dict) -> Iterable:
        author_value = self.value_at(dotted_name, spec) or []
        if isinstance(author_value, str):
            yield author_value
        else:
            yield from author_value

    def validate_publication_authors(self, spec: dict) -> List[str]:
        errors = []
        for publication_spec in self.value_at("publication", spec, default=[]):
            context = f'publication[title: "{publication_spec["title"]}"].author'
            author_list = list(self._person_ref_list("author", publication_spec))

            for person_ref in author_list:
                errors += self._validate_person_reference(person_ref, spec, context)

            corresponding_author_ref = self.value_at("corresponding_author", publication_spec)
            if corresponding_author_ref and corresponding_author_ref not in author_list:
                errors += [f"Reference error: reference to undefined person "
                           f"({corresponding_author_ref}) in {context}"]
        return errors

    def validate_dataset_authors(self, spec: dict) -> List[str]:
        errors = []
        for person_ref in self._person_ref_list("dataset.author", spec):
            errors += self._validate_person_reference(person_ref, spec, f"dataset.author")
        return errors

    def validate_study_contributors(self, spec: dict) -> List[str]:
        errors = []
        contributor_path = "study.contributor"
        for person_ref in self._person_ref_list(contributor_path, spec):
            errors += self._validate_person_reference(person_ref, spec, contributor_path)
        return errors

    def perform_validation(self, spec: dict) -> List:
        errors = self.validate_publication_authors(spec)
        errors += self.validate_dataset_authors(spec)
        errors += self.validate_study_contributors(spec)

        pi_path = "study.principal_investigator"
        errors += self._validate_person_reference(self.value_at(pi_path, spec), spec, pi_path)

        return errors
