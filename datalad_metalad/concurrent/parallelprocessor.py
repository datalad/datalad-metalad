import logging
from copy import deepcopy
from typing import (
    Any,
    Callable,
    List,
    Optional,
)

from .processor import Processor


__docformat__ = "restructuredtext"


class ParallelProcessor:
    """
    Run all processors and process the individual results, by
    calling the result handler with the callable, and the
    callback arguments.
    """
    def __init__(self,
                 processors: List[Processor],
                 name: Optional[str] = None):

        self.processors = processors
        self.name = name or str(id(self))

        self.result_processor = None
        self.result_processor_args = None

    def __repr__(self):
        return f"<{type(self).__name__}[{self.name}], processors[{len(self.processors)}]>"

    def start(self,
              arguments: List[Any],
              result_processor: Callable,
              result_processor_args: Optional[List[Any]] = None,
              sequential: bool = False):

        self.result_processor = result_processor
        self.result_processor_args = result_processor_args or []
        for processor in self.processors:
            logging.debug(f"PPP {self.name}: starting: {repr(processor)}")
            processor.start(arguments=deepcopy(arguments),
                            result_processor=self._downstream_result_processor,
                            sequential=sequential)

    def _downstream_result_processor(self, result_type, result):
        """process result from one of the processes"""
        logging.debug(f"{self}: downstream result processor called with {result_type}, {result}")
        logging.debug(f"{self}: calling client result processor {self.result_processor}")
        self.result_processor(result_type, result, *self.result_processor_args)
