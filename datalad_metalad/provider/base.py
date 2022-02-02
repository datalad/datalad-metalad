import abc
from typing import Iterable

from ..pipelineelement import PipelineElement


class Provider(PipelineElement, metaclass=abc.ABCMeta):
    def __init__(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def next_object(self) -> Iterable:
        raise NotImplementedError
