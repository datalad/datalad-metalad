import logging
import os
import sys
import time
from concurrent.futures import (
    CancelledError,
    Executor,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
)
from copy import deepcopy
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
)
from pprint import pprint
from datalad_metalad.concurrent.processor import (
    FutureSet,
    ProcessorResult,
    ProcessorResultType,
)


__docformat__ = "restructuredtext"


logging.basicConfig(level=logging.DEBUG)

def xxprint(*args, **kwargs):
    logging.debug(*args)


class SProcessor:
    def __init__(self,
                 a_callable: Callable,
                 executor: Executor,
                 future_set: FutureSet,
                 name: Optional[str] = None):

        self.callable = a_callable
        self.executor = executor
        self.future_set = future_set
        self.name = name or str(id(self))

        self.result_processor = None
        self.result_processor_args = None

    def __repr__(self):
        return f"XXX {self.name}:"

    def start(self,
              arguments: List[Any],
              result_processor: Callable,
              result_processor_args: Optional[List[Any]] = None):
        """

        :param arguments:
        :param result_processor:
        :param result_processor_args:
        :return:
        """
        self.result_processor = result_processor
        self.result_processor_args = result_processor_args or []

        logging.debug(f"SXXX {self.name}:{self.callable} start called with {arguments}")
        future = self.executor.submit(self.callable, *arguments)
        self.future_set.add_future(future, self)

    def done_handler(self, done_future: Future):
        """process a done future

        Retrieve the result from the executor, check for exceptions,
        enclose everything in a ProcessorResult.

        Call the result handler method

        :param done_future: the future that is done
        """
        try:
            logging.debug(f"XXX {self.name}: {self.result_processor}")
            callable_result = done_future.result()
            logging.debug(f"XXX {self.name}: DONE: subprocess returned: {repr(callable_result)}")
            self.result_processor(
                ProcessorResultType.Result,
                callable_result,
                *self.result_processor_args)
        except CancelledError as exception:
            logging.debug(f"XXX {self.name}: CANCELLED: repr(self.callable)")
            self.result_processor(
                ProcessorResultType.Cancelled,
                exception,
                *self.result_processor_args)
        except:
            logging.debug(f"XXX {self.name}: EXCEPTION: repr(self.callable)")
            exception = done_future.exception(timeout=0)
            self.result_processor(
                ProcessorResultType.Exception,
                exception,
                *self.result_processor_args)


class SProcessorParallel:
    """
    Run all processors and process the individual results, by
    calling the result handler with the callable, and the
    callback arguments.
    """
    def __init__(self,
                 processors: List[SProcessor],
                 executor: Executor,
                 future_set: FutureSet,
                 name: Optional[str] = None):

        self.processors = processors
        self.executor = executor
        self.future_set = future_set
        self.name = name or str(id(self))

        self.client_result_processor = None
        self.client_result_processor_args = None

    def __repr__(self):
        return f"PPP {self.name}"

    def start(self,
              arguments: List[Any],
              client_result_processor,
              client_result_processor_args: Optional[List[Any]] = None):

        self.client_result_processor = client_result_processor
        self.client_result_processor_args = client_result_processor_args or []
        for processor in self.processors:
            logging.debug(f"PPP {self.name}: starting: {repr(processor)}")
            processor.start(
                deepcopy(arguments),
                self.result_processor,
                self.client_result_processor_args)

    def result_processor(self, result_type, result):
        """process result from one of the processes"""
        logging.debug(f"PPP {self.name}: result processor called with {result_type}, {result}")
        logging.debug(f"PPP {self.name}: calling client result processor {self.client_result_processor}")
        self.client_result_processor(result_type, result, *self.client_result_processor_args)


class SProcessorSequential:
    """
    Run all processors and process the individual results, by
    calling the result handler with the callable, and the
    callback arguments.
    """
    def __init__(self,
                 processors: List[SProcessor],
                 executor: Executor,
                 future_set: FutureSet,
                 name: Optional[str] = None):

        self.processors = processors
        self.executor = executor
        self.future_set = future_set
        self.name = name or str(id(self))

        self.client_result_processor = None
        self.client_result_processor_args = None

    def __repr__(self):
        return f"SSS {self.name}:"

    def result_processor(self,
                         result_type: ProcessorResultType,
                         result: Any,
                         index: int):

        if result_type != ProcessorResultType.Result or index == len(self.processors):
            logging.debug(f"SSS {self.name}: calling {self.client_result_processor}")
            self.client_result_processor(
                result_type,
                result,
                *self.client_result_processor_args)
        else:
            logging.debug(f"SSS {self.name}: starting processor[{index}] with argument: {[result]}")
            processor = self.processors[index]
            processor.start([result], self.result_processor, [index + 1])

    def start(self,
              arguments: List[Any],
              client_result_processor: Callable,
              client_result_processor_args: Optional[List[Any]] = None):

        self.client_result_processor = client_result_processor
        self.client_result_processor_args = client_result_processor_args or []

        processor = self.processors[0]
        logging.debug(f"SSS {self.name}: starting processor[0] with argument: {arguments}")
        processor.start(arguments, self.result_processor, [1])


def add_component(result_type: ProcessorResultType, pe: Dict) -> Any:
    logging.debug(f"add_component ENTER: {result_type}, {pe}")
    if "name_list" not in pe:
        pe["name_list"] = f"a {os.getpid()} {time.time()}"
    else:
        pe["name_list"] += f" b {os.getpid()} {time.time()}"
    logging.debug(f"add_component EXIT: {os.getpid()}, {pe}")
    print(f"FINAL {pe}")


def add_b_component(result_type: ProcessorResultType, pe: Dict) -> Any:
    logging.debug(f"add_b_component ENTER: {result_type}, {pe}")
    time.sleep(2)
    if "name_list" not in pe:
        pe["name_list"] = f"c {os.getpid()} {time.time()}"
    else:
        pe["name_list"] += f" d {os.getpid()} {time.time()}"
    logging.debug(f"add_b_component EXIT: {os.getpid()}, {pe}")


def sequential_callable(pipeline_element: Dict) -> Any:
    logging.debug(f"sequential_callable ENTER: {pipeline_element}")
    if "name_list" not in pipeline_element:
        pipeline_element["name_list"] = f"c {os.getpid()} {time.time()}"
    else:
        pipeline_element["name_list"] += f" d {os.getpid()} {time.time()}"
    logging.debug(f"sequential_callable EXIT: {os.getpid()}, {pipeline_element}")
    return pipeline_element


def final_result_handler(*args):
    logging.debug(f"FINAL RESULT: {args}")


def test_sp_par_basics():
    #executor = ProcessPoolExecutor(10)
    executor = ThreadPoolExecutor(10)
    future_set = FutureSet()

    processors = [
        SProcessorSequential(
            [SProcessor(sequential_callable, executor, future_set) for _ in range(4)],
            executor,
            future_set)
        for _ in range(3)
    ]

    par = SProcessorParallel(
        processors=processors,
        executor=executor,
        future_set=future_set
    )

    print(f"main: {os.getpid()}")
    pipeline_element = {"name_list": "input"}
    par.start([pipeline_element], add_component, [])

    for done_future, done_sprocessor in future_set.done:
        done_sprocessor.done_handler(done_future)


test_sp_par_basics()
