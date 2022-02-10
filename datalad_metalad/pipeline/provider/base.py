import abc
from typing import Iterable

from ..pipelineelement import PipelineElement


class Provider(PipelineElement, metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def next_object(self) -> Iterable:
        raise NotImplementedError
