import re
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, List, Union


ATTACHED_INDEX_PATTERN = r"([a-z_@A-Z0-9\-])*\[([0-9]+)\]"


class NoneElement(object):
    pass


class ContentValidator(ABC):

    NonExistingElement = NoneElement()

    @abstractmethod
    def perform_validation(self, spec: dict) -> List:
        pass

    @staticmethod
    def values_with_key(searched_key: str, spec: Dict) -> Iterable:
        """
        Iterator over all values in the dictionary that are associated with searched_key.
        The iterator will not return nested appearances of searched_key.
        """
        for key, value in spec.items():
            if key == searched_key:
                yield value
            else:
                if isinstance(value, Dict):
                    for sub_value in ContentValidator.values_with_key(searched_key, value):
                        yield sub_value

    @staticmethod
    def path_to_dotted_name(path: List[Union[int, str]]) -> str:
        return "".join([
            (f"{path_element}" if index == 0 else f".{path_element}")
            if isinstance(path_element, str) else f"[{path_element}]"
            for index, path_element in enumerate(path)])

    @staticmethod
    def dotted_name_to_path(dotted_name: str) -> List[Union[int, str]]:
        result = []
        parts = dotted_name.split(".")
        for part in parts:
            match = re.match(ATTACHED_INDEX_PATTERN, part)
            if match:
                if match.group(1):
                    result += [match.group(1), int(match.group(2))]
                else:
                    result += [int(match.group(2))]
            else:
                result += [part]
        return result

    @staticmethod
    def get_element_at_path(dotted_name: str, spec: Union[dict, list]) -> Any:
        current_object = spec
        for key_or_index in ContentValidator.dotted_name_to_path(dotted_name):
            if isinstance(key_or_index, int) and isinstance(current_object, list):
                try:
                    current_object = current_object[key_or_index]
                except IndexError:
                    return ContentValidator.NonExistingElement
            elif isinstance(key_or_index, str):
                if key_or_index not in current_object:
                    return ContentValidator.NonExistingElement
                current_object = current_object[key_or_index]
        return current_object

    @staticmethod
    def has_path_element(dotted_name: str, spec: Union[dict, list]) -> bool:
        """
        Return True if the element at the path represented by dotted_name exists
        in spec, else False.
        The dotted name is a sequence of key-names or list elements, joined by ".".
        List elements are identified by "[x]", where x is the requested index.

        For example, "a.b.c" would retrieve spec["a"]["b"]["c"], and "x.[0].y" would
        retrieve spec["x"][0]["y"].
        """
        return ContentValidator.get_element_at_path(dotted_name, spec) is not ContentValidator.NonExistingElement

    @staticmethod
    def value_at(dotted_name: str, spec: Union[dict, list], default=None) -> Union[Any, None]:
        """
        Return the value stored in spec at the path represented by dotted_name.
        If the path specificed by the dotted name does not exist in spec, "default" is returned.
        """
        value = ContentValidator.get_element_at_path(dotted_name, spec)
        if value is ContentValidator.NonExistingElement:
            return default
        return value

    @staticmethod
    def publications(spec: Dict) -> Iterable:
        publication_key = "publication"
        for index, publication_spec in enumerate(ContentValidator.value_at(publication_key, spec, default=[])):
            path = publication_key + f"[{index}]"
            yield path, publication_spec

    def __init__(self, file_name: str):
        self.file_name = file_name
