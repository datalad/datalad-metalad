import abc
from typing import (
    Any,
    Tuple,
)

from ..pipelineelement import PipelineElement


class Processor(metaclass=abc.ABCMeta):
    """ A processor for conduct """
    def __init__(self, *args, **kwargs):
        pass

    def execute(self,
                context: Any,
                pipeline_element: PipelineElement
                ) -> Tuple[Any, PipelineElement]:
        """
        Execute the processor. If we want to use process worker pools,
        we cannot return an iterator or generator as a result.
        Therefore this method will collect all results from self.process
        and return them in a list of tuples, which consist of the passed
        context and .
        """
        return context, self.process(pipeline_element)

    @abc.abstractmethod
    def process(self, pipeline_element: PipelineElement) -> PipelineElement:
        """
        Overwrite this method in derived classes to implement
        the functionality of the processor. Return-values are
        feed into the next processor in the pipeline or returned
        as result of a datalad command, usually "meta-conduct".
        """
        raise NotImplementedError
