import abc
import dataclasses
import enum
import logging
from concurrent.futures import (
    CancelledError,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    TimeoutError,
    as_completed,
)
from typing import (
    Any,
    Callable,
    List,
    NamedTuple,
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


@dataclasses.dataclass()
class SequentialFuture:
    _result: Any
    _exception: Exception = None

    def result(self):
        if self._exception is not None:
            raise self._exception
        return self._result

    def exception(self):
        return self._exception

    def __hash__(self):
        return id(self)


class FutureSet(dict):
    def __init__(self):
        dict.__init__(self)

    def add_future(self,
                   future: Future,
                   processor: ProcessorInterface):
        self[future] = processor

    def remove_future(self, future: Future):
        del self[future]


class Processor(ProcessorInterface):

    executor = ProcessPoolExecutor(16)
    future_set = FutureSet()
    started = False

    sequential_future_set = FutureSet()

    @staticmethod
    def done(timeout: Optional[float] = None):

        future_set = Processor.future_set
        while future_set:
            handled_futures = set()
            try:
                for future in as_completed(fs=future_set.keys(),
                                           timeout=timeout):
                    yield future, future_set[future]
                    handled_futures.add(future)

            except TimeoutError:
                break

            finally:
                for future in handled_futures:
                    future_set.remove_future(future)

        sequential_future_set = Processor.sequential_future_set
        while sequential_future_set:
            for sequential_future, processor in tuple(sequential_future_set.items()):
                yield sequential_future, processor
                sequential_future_set.remove_future(sequential_future)

    @staticmethod
    def done_all(timeout: Optional[float] = None):
        for future, processor in Processor.done(timeout):
            yield processor.done_handler(future)

    @classmethod
    def _check_start_state(cls, method_name: str):
        if cls.started is True:
            raise RuntimeError(f"Cannot execute '{method_name}' after "
                               f"Processors have been started")

    @classmethod
    def set_process_executor(cls, max_worker: int = 16):
        if isinstance(cls.executor, ProcessPoolExecutor):
            return
        cls._check_start_state("Process.set_process_executor")
        cls.executor = ProcessPoolExecutor(max_worker)

    @classmethod
    def set_thread_executor(cls, max_worker: int = 8):
        if isinstance(cls.executor, ThreadPoolExecutor):
            return
        cls._check_start_state("Process.set_process_executor")
        cls.executor = ThreadPoolExecutor(max_worker)

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
        Processor.started = True

        self.result_processor = result_processor
        self.result_processor_args = result_processor_args or []

        logging.debug(f"{self}: start called with arguments: {arguments}")

        if sequential is True:
            exception = None
            try:
                result = self.callable(*arguments)
            except Exception as exc:
                result = None
                exception = exc
            sequential_future = SequentialFuture(result, exception)
            self.sequential_future_set.add_future(sequential_future, self)

        else:
            future = self.executor.submit(self.callable, *arguments)
            self.future_set.add_future(future, self)

    def done_handler(self, done_future: Future) -> Any:
        """process a done future

        Retrieve the result from the executor, check for exceptions,
        enclose everything in a ProcessorResult.

        Call the result handler method

        :param done_future: the future that is done
        :return: the object returned from the result_handler
        :rtype: Any
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

        return self.result_processor(self,
                                     result_type,
                                     result,
                                     *self.result_processor_args)


class InterfaceExtendedProcessor(Processor):
    """
    A processor that will invoke the given callable with itself as the
    first arguments.
    """
    def __init__(self,
                 wrapped_callable: Callable,
                 name: Optional[str] = None):

        Processor.__init__(self, self.interface_extender, name)
        self.wrapped_callable = wrapped_callable

    def __repr__(self):
        return f"InterfaceExtendedProcessor[{self.name}]"

    def interface_extender(self, *args, **kwargs) -> Any:
        return self.wrapped_callable(self, *args, **kwargs)
