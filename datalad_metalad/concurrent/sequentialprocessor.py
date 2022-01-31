import logging
from typing import (
    Any,
    Callable,
    List,
    Optional,
)

from .processor import (
    Processor,
    ProcessorResultType,
)


__docformat__ = "restructuredtext"


class SequentialProcessor:
    """
    Run all processors and process the individual results, by
    calling the result handler with the callable, and the
    callback arguments.
    """
    def __init__(self,
                 processors: List[Processor],
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

    def _downstream_result_processor(self,
                                     result_type: ProcessorResultType,
                                     result: Any,
                                     index: int,
                                     sequential: bool):

        if result_type != ProcessorResultType.Result \
                or index == len(self.processors):

            logging.debug(
                f"{self}: calling client result "
                f"processor {self.result_processor}")
            self.result_processor(
                result_type,
                result,
                *self.result_processor_args)
        else:
            logging.debug(
                f"{self}: starting processor[{index}] "
                f"with argument: {[result]}")
            processor = self.processors[index]
            processor.start(arguments=[result],
                            result_processor=self._downstream_result_processor,
                            result_processor_args=[index + 1, sequential],
                            sequential=sequential)

    def start(self,
              arguments: List[Any],
              result_processor: Callable,
              result_processor_args: Optional[List[Any]] = None,
              sequential: bool = False):
        """

        :param arguments:
        :param result_processor:
        :param result_processor_args:
        :param sequential:
        :return:
        """
        self.result_processor = result_processor
        self.result_processor_args = result_processor_args or []

        processor = self.processors[0]
        logging.debug(
            f"{self}: starting processor[0] with argument: {arguments}")
        processor.start(arguments=arguments,
                        result_processor=self._downstream_result_processor,
                        result_processor_args=[1, sequential],
                        sequential=sequential)
