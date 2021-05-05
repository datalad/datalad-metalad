import abc
from typing import Any, Iterable, List, Tuple


class Processor(metaclass=abc.ABCMeta):
    """ A processor for conduct """
    def __init__(self, *args, **kwargs):
        pass

    def execute(self, context: Any, *args, **kwargs) -> Tuple[Any, List[Any]]:
        """
        Execute the processor. If we want to use process worker pools,
        we cannot return an iterator or generator as a result.
        Therefore this method will collect all results from self.process
        and return them in a list of tuples, which consist of the passed
        context and .
        """
        return context, list(self.process(*args, **kwargs))

    @abc.abstractmethod
    def process(self, *args, **kwargs) -> Iterable:
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
