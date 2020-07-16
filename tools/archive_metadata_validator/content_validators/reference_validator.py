from collections import namedtuple
from typing import Iterable, List, Union

from messages import ValidatorMessage, ErrorMessage, ObjectLocation
from .content_validator import ContentValidator


PersonInfo = namedtuple("PersonInfo", ["first_name", "last_name", "id"])


class ReferenceValidator(ContentValidator):
    def _validate_person_reference(self, person_ref: str, referrer_name: str) -> List[ValidatorMessage]:
        if self.has_element_at_dotted_name(f"person.{ContentValidator.escape_name(person_ref)}") is False:
            return [
                ErrorMessage(
                    f"reference to undefined person ({person_ref}) in {referrer_name}",
                    ObjectLocation(self.file_name, referrer_name, self.object_locations))]
        return []

    def _person_reference_list(self, referrer_name: str) -> Iterable:
        author_value = self.value_at(referrer_name, default=[])
        if isinstance(author_value, str):
            yield author_value
        else:
            yield from author_value

    def validate_corresponding_author(self, publication_path: List[Union[int, str]]):
        coa_dotted_name = ContentValidator.path_to_dotted_name(publication_path + ["corresponding_author"])
        coa_ref = self.value_at(coa_dotted_name)
        if coa_ref:
            author_dotted_name = self.path_to_dotted_name(publication_path + ["author"])
            author_list = list(self._person_reference_list(author_dotted_name))
            if coa_ref not in author_list:
                return [
                    ErrorMessage(
                        f"author specified in {coa_dotted_name} ({coa_ref}) is not in {author_dotted_name}",
                        ObjectLocation(self.file_name, coa_dotted_name, self.object_locations))]
        return []

    def validate_publication_authors(self) -> List[ValidatorMessage]:
        messages = []
        for index, publication_spec in enumerate(self.value_at("publication", default=[])):
            referrer_name = self.path_to_dotted_name(["publication", index, "author"])
            messages += self.validate_person_referrer(referrer_name)
            messages += self.validate_corresponding_author(["publication", index])
        return messages

    def validate_person_referrer_list(self, referrer_name, person_reference_list):
        messages = []
        for person_reference in person_reference_list:
            messages += self._validate_person_reference(person_reference, referrer_name)
        return messages

    def validate_person_referrer(self, referrer_name: str) -> List[ValidatorMessage]:
        return self.validate_person_referrer_list(referrer_name, self._person_reference_list(referrer_name))

    def perform_validation(self) -> List[ValidatorMessage]:
        messages = self.validate_publication_authors()
        messages += self.validate_person_referrer("dataset.author")
        messages += self.validate_person_referrer("study.contributor")

        pi_path = "study.principal_investigator"
        if not self.has_element_at_dotted_name(pi_path):
            messages += [
                ErrorMessage(
                    f"missing key '{pi_path}'",
                    ObjectLocation(self.file_name, pi_path, self.object_locations))]
        else:
            messages += self._validate_person_reference(self.value_at(pi_path), pi_path)
        return messages
