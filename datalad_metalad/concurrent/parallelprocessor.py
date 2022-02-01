import logging
from copy import deepcopy
from typing import (
    Any,
    Callable,
    List,
    Optional,
)

from .processor import ProcessorInterface


__docformat__ = "restructuredtext"


class ParallelProcessor(ProcessorInterface):
    """
    Run all processors and process the individual results, by
    calling the result handler with the callable, and the
    callback arguments.
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
        for processor in self.processors:
            logging.debug(f"{self}: starting: {repr(processor)}")
            processor.start(arguments=arguments,
                            result_processor=self._downstream_result_processor,
                            sequential=sequential)

    def _downstream_result_processor(self, sender, result_type, result):
        """ process result from one of the processes

        :param sender:
        :param result_type:
        :param result:
        """
        logging.debug(f"{self}: ParallelProcessor._downstream_result_processor"
                      f"({sender}, {result_type}, {result}) called")
        logging.debug(f"{self}: calling {self.result_processor} with "
                      f"({sender}, {result_type}, {result}, "
                      f"*{self.result_processor_args})")
        self.result_processor(sender,
                              result_type,
                              result,
                              *self.result_processor_args)
