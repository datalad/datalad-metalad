import re
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, List, Optional, Union

from messages import ValidatorMessage


class NoneElement(object):
    pass


class ContentValidator(ABC):

    IndexPattern = r"\[([0-9]+)\]"
    NonExistingElement = NoneElement()

    @staticmethod
    def escape_name(name: str) -> str:
        """ Escape . and backslashes in the name in the dotted name format """
        name = name.replace("\\", "\\\\")
        return name.replace(".", "\\.")

    @staticmethod
    def path_to_dotted_name(path: List[Union[int, str]]) -> str:
        """ Convert a path to a dotted name, escape . and backslashes """
        dotted_name_elements = []
        for path_element in path:
            if isinstance(path_element, int):
                dotted_name_element = f"[{path_element}]"
            elif isinstance(path_element, str):
                dotted_name_element = ContentValidator.escape_name(path_element)
            else:
                raise TypeError(f"type of path element `{path_element}´ ({type(path_element)}) is not supported")
            dotted_name_elements.append(dotted_name_element)
        return ".".join(dotted_name_elements)

    @staticmethod
    def dotted_name_to_path(dotted_name: str) -> List[Union[int, str]]:
        """ Convert a dotted name to a path, honouring escaped . and backslashes """
        result = []
        previous_character = ""
        current_path_element = ""
        for character in dotted_name:
            if previous_character == "\\":
                if character == ".":
                    current_path_element += "."
                elif character == "\\":
                    current_path_element += "\\"
                else:
                    raise Exception(f"Encoding error in dotted name: {dotted_name}")
                previous_character = ""
            else:
                if character == ".":
                    match = re.match(ContentValidator.IndexPattern, current_path_element)
                    if match:
                        result.append(int(match.group(1)))
                    else:
                        result.append(current_path_element)
                    current_path_element = ""
                elif character != "\\":
                    current_path_element += character
                previous_character = character
        result.append(current_path_element)
        return result

    @staticmethod
    def _values_with_key(searched_key: str, spec: Union[dict, list]) -> Iterable:
        """
        Iterator over all values in the dictionary that are associated with searched_key.
        The iterator will not return nested appearances of searched_key.
        """
        for key, value in spec.items():
            if key == searched_key:
                yield value
            else:
                if isinstance(value, Dict):
                    for sub_value in ContentValidator._values_with_key(searched_key, value):
                        yield sub_value

    @abstractmethod
    def perform_validation(self) -> List[ValidatorMessage]:
        pass

    def values_with_key(self, searched_key: str) -> Iterable:
        """
        Iterator over all values in the dictionary that are associated with searched_key.
        The iterator will not return nested appearances of searched_key.
        """
        return ContentValidator._values_with_key(searched_key, self.spec)

    def get_element_at_path_in_spec(self, path: List[Union[int, str]], spec: Union[dict, list]) -> Any:
        current_object = spec
        for key_or_index in path:
            if isinstance(key_or_index, int) and isinstance(current_object, list):
                try:
                    current_object = current_object[key_or_index]
                except IndexError:
                    return self.NonExistingElement
            elif isinstance(key_or_index, str):
                if key_or_index not in current_object:
                    return self.NonExistingElement
                current_object = current_object[key_or_index]
        return current_object

    def get_element_at_dotted_name_in_spec(self, dotted_name: str, spec: Union[dict, list]) -> Any:
        return self.get_element_at_path_in_spec(self.dotted_name_to_path(dotted_name), spec)

    def get_element_at_dotted_name(self, dotted_name: str) -> Any:
        return self.get_element_at_dotted_name_in_spec(dotted_name, self.spec)

    def has_element_at_dotted_name(self, dotted_name: str) -> bool:
        """
        Return True if the element at the path represented by dotted_name exists
        in spec, else False.
        The dotted name is a sequence of key-names or list elements, joined by ".".
        List elements are identified by "[x]", where x is the requested index.

        For example, "a.b.c" would retrieve spec["a"]["b"]["c"], and "x.[0].y" would
        retrieve spec["x"][0]["y"].
        """
        return self.get_element_at_dotted_name(dotted_name) is not self.NonExistingElement

    def value_at_in_spec(self, dotted_name: str, spec: Union[dict, str], default=None) -> Union[Any, None]:
        """
        Return the value stored in spec at the path represented by dotted_name.
        If the path specified by the dotted name does not exist in spec, "default" is returned.
        """
        value = self.get_element_at_dotted_name_in_spec(dotted_name, spec)
        if value is ContentValidator.NonExistingElement:
            return default
        return value

    def value_at_path_in_spec(self,
                              path: List[Union[int, str]],
                              spec: Union[dict, str],
                              default=None) -> Union[Any, None]:
        """
        Return the value stored in spec at the path.
        If the path does not exist in spec, "default" is returned.
        """
        value = self.get_element_at_path_in_spec(path, spec)
        if value is ContentValidator.NonExistingElement:
            return default
        return value

    def value_at_path(self, path: List[Union[int, str]], default=None) -> Union[Any, None]:
        return self.value_at_path_in_spec(path, self.spec, default)

    def value_at(self, dotted_name: str, default=None) -> Union[Any, None]:
        return self.value_at_in_spec(dotted_name, self.spec, default)

    def publications(self) -> Iterable:
        publication_key = "publication"
        for index, publication_spec in enumerate(self.value_at(publication_key, default=[])):
            publication_dotted_name = self.path_to_dotted_name([publication_key, index])
            yield publication_dotted_name, publication_spec

    def __init__(self, file_name: str, spec: Union[dict, list], object_locations: Optional[dict] = None):
        self.file_name = file_name
        self.spec = spec
        self.object_locations = {} if object_locations is None else object_locations


class ContentValidatorInfo(object):
    def __init__(self,
                 validator_class: type(ContentValidator),
                 args: Optional[list] = None,
                 kwargs: Optional[dict] = None):

        self.validator_class = validator_class
        self.args = args if args else []
        self.kwargs = kwargs if kwargs else {}

    def create(self, file_name: str, spec: Union[dict, list], object_locations: dict) -> type(ContentValidator):
        return self.validator_class(file_name, spec, object_locations, *self.args, **self.kwargs)
