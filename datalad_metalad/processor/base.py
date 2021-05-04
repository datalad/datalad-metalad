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
        """
        Overwrite this method in derived classes to implement
        the functionality of the processor. Return-values are
        feed into the next processor in the pipeline or returned
        as result of a datalad command, usually "meta-conduct".
        """
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def input_type() -> str:
        """
        The input "type" of this processor. This is a free-text
        string that serves to verify whether two processors can be
        connected. It has not significance to python.
        The input type of a processor is compared to the output
        type of its predecessor, which is either a processor or
        a provider.
        """
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def output_type() -> str:
        """
        The output "type" of this processor. This is a free-text
        string that serves to verify whether two processors can be
        connected. It has not significance to python.
        The output type of a processor is compared to the input
        type of its successor, if it exists.
        """
        raise NotImplementedError
