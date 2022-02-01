import abc
import enum
import logging
from concurrent.futures import (
    CancelledError,
    Future,
    ProcessPoolExecutor,
    TimeoutError,
    as_completed,
)
from typing import (
    Any,
    Callable,
    List,
    Optional,
)


__docformat__ = "restructuredtext"


class ProcessorResultType(enum.Enum):
    Result = 0
    Exception = 1
    Cancelled = 2


class ProcessorInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def start(self,
              arguments: List[Any],
              result_processor: Callable,
              result_processor_args: Optional[List[Any]] = None,
              sequential: bool = False):
        raise NotImplementedError


class FutureSet:
    def __init__(self):
        self.futures = dict()

    def add_future(self,
                   future: Future,
                   processor: ProcessorInterface):
        self.futures[future] = processor

    def remove_future(self, future: Future):
        del self.futures[future]

    @property
    def done(self):
        while self.futures:
            for future in as_completed(self.futures.keys()):
                yield future, self.futures[future]
                self.remove_future(future)


class Processor(ProcessorInterface):

    executor = ProcessPoolExecutor(16)
    future_set = FutureSet()

    @staticmethod
    def done(timeout: Optional[float] = None):
        while Processor.future_set.futures:
            try:
                for future in as_completed(
                                    fs=Processor.future_set.futures.keys(),
                                    timeout=timeout):
                    yield future, Processor.future_set.futures[future]
                    Processor.future_set.remove_future(future)
            except TimeoutError:
                return

    def __init__(self,
                 a_callable: Callable,
                 name: Optional[str] = None):
        """

        :param a_callable:
        :param name:
        """
        self.callable = a_callable
        self.name = name or str(id(self))
        self.result_processor = None
        self.result_processor_args = None

    def __repr__(self):
        return f"<{type(self).__name__}[{self.name}], {self.callable}>"

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
        :return: None
        """
        self.result_processor = result_processor
        self.result_processor_args = result_processor_args or []

        logging.debug(f"{self}: start called with arguments: {arguments}")
        if sequential is True:
            result = self.callable(self, *arguments)
            self.result_processor(
                ProcessorResultType.Result,
                result,
                *self.result_processor_args)
        else:
            future = self.executor.submit(self.callable, self, *arguments)
            self.future_set.add_future(future, self)

    def done_handler(self, done_future: Future):
        """process a done future

        Retrieve the result from the executor, check for exceptions,
        enclose everything in a ProcessorResult.

        Call the result handler method

        :param done_future: the future that is done
        """
        try:
            result = done_future.result()
            logging.debug(f"{self} future returned with: {result}")
            result_type = ProcessorResultType.Result

        except CancelledError as exception:
            logging.debug(f"{self} future raised: {exception}")
            result_type = ProcessorResultType.Cancelled
            result = exception

        except Exception as exception:
            logging.debug(f"{self} future raised: {exception}")
            result_type = ProcessorResultType.Exception
            result = exception

        self.result_processor(self,
                              result_type,
                              result,
                              *self.result_processor_args)
