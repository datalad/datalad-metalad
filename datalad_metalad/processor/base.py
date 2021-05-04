import abc
from typing import Any, Tuple


class Processor(metaclass=abc.ABCMeta):
    """ A processor for conduct """
    def __init__(self, *args, **kwargs):
        pass

    def execute(self, context: Any, *args, **kwargs) -> Tuple[Any, Any]:
        process_result = self.process(*args, **kwargs)
        return context, process_result

    @abc.abstractmethod
    def process(self, *args, **kwargs) -> Any:
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def input_type() -> str:
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def output_type() -> str:
        raise NotImplementedError
