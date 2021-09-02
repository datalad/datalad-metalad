import abc
from typing import Iterable


class Provider(metaclass=abc.ABCMeta):
    def __init__(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def next_object(self) -> Iterable:
        raise NotImplementedError
