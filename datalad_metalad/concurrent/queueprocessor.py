import logging
from typing import (
    Any,
    Callable,
    List,
    Optional,
)

from .processor import (
    ProcessorInterface,
    ProcessorResultType,
)


__docformat__ = "restructuredtext"


class QueueProcessor(ProcessorInterface):
    """
    A queue that will execute a sequence of processors on an
    object that is given to it. The result of processor n is
    handed as input to processor n+1. The final result is processed
    by the result processor given in the start-methos.

    Processors are created by supplying a factory method and arguments
    per processor.
    """

    def __init__(self,
                 processors: List[ProcessorInterface],
                 name: Optional[str] = None):
        """

        :param processors:
        :param name:
        """
        self.processors = processors
        self.name = name or str(id(self))

        self.result_processor = None
        self.result_processor_args = None
        self.last_processor = None
        self.last_result = None

    def __repr__(self):
        return f"<{type(self).__name__}[{self.name}], " \
               f"processors[{len(self.processors)}]>"

    def start(self,
              arguments: List[Any],
              result_processor: Callable,
              result_processor_args: Optional[List[Any]] = None,
              sequential: bool = False):

        self.result_processor = result_processor
        self.result_processor_args = result_processor_args or []

        self.last_processor = self.processors[0]
        logging.debug(f"{self}: starting: {self.last_processor}")
        self.last_processor.start(
            arguments=arguments,
            result_processor=self._downstream_result_processor,
            result_processor_args=[1, sequential],
            sequential=sequential)

    def _downstream_result_processor(self,
                                     _: ProcessorInterface,
                                     result_type: ProcessorResultType,
                                     result: Any,
                                     index: int,
                                     sequential: bool):

        if result_type != ProcessorResultType.Result \
                or index == len(self.processors):

            if result_type != ProcessorResultType.Result:
                result = self.last_result

            logging.debug(
                f"{self}: calling client result "
                f"processor {self.result_processor}")
            self.result_processor(
                self.last_processor,
                result_type,
                result,
                *self.result_processor_args)
        else:
            self.last_result = result
            self.last_processor = self.processors[index]

            logging.debug(f"{self}: starting:"
                          f" {self.last_processor}"
                          f" with {[result]}")
            self.last_processor.start(
                arguments=[result],
                result_processor=self._downstream_result_processor,
                result_processor_args=[index + 1, sequential],
                sequential=sequential)
