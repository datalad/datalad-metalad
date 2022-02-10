import abc
from typing import (
    Any,
    Tuple,
)

from ..pipelinedata import PipelineData
from ..pipelineelement import PipelineElement


class Processor(PipelineElement, metaclass=abc.ABCMeta):
    """ A processor for conduct """

    def execute(self,
                context: Any,
                pipeline_data: PipelineData
                ) -> Tuple[Any, PipelineData]:
        """
        Execute the processor. If we want to use process worker pools,
        we cannot return an iterator or generator as a result.
        Therefore this method will collect all results from self.process
        and return them in a list of tuples, which consist of the passed
        context and .
        """
        return context, self.process(pipeline_data)

    @abc.abstractmethod
    def process(self, pipeline_data: PipelineData) -> PipelineData:
        """
        Overwrite this method in derived classes to implement
        the functionality of the processor. Return-values are
        feed into the next processor in the pipeline or returned
        as result of a datalad command, usually "meta-conduct".
        """
        raise NotImplementedError
