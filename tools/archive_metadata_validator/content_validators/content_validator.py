from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, List, Union


class ContentValidator(ABC):
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
    def value_at(dotted_name: str, spec: dict, default=None) -> Union[Any, None]:
        """
        Return the value stored in spec at the key-path defined by dotted name, which
        defines the keys by concatenating them with ".".  For example, e.g. "a.b.c"
        would retrieve spec["a"]["b"]["c"]. If the path specificed by the dotted name
        does not exist in spec, "default" is returned.
        """
        value = None
        for key in dotted_name.split("."):
            value = spec.get(key, None)
            if value is None:
                return default
            spec = value
        return value
