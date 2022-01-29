import os
import time
import logging
from typing import (
    Any,
    Dict,
)

from ..processor import (
    Processor,
    ProcessorResultType,
)
from ..parallelprocessor import ParallelProcessor
from ..sequentialprocessor import SequentialProcessor


def parallel_result_handler(result_type: ProcessorResultType, pe: Dict):
    logging.debug(f"parallel_result_handler ENTER: {result_type}, {pe}")
    if "name_list" not in pe:
        pe["name_list"] = f"a {os.getpid()} {time.time()}"
    else:
        pe["name_list"] += f" b {os.getpid()} {time.time()}"
    logging.debug(f"parallel_result_handler EXIT: {os.getpid()}, {pe}")
    print(f"FINAL {pe}")


def test_sp_par_basics():

    def individual_worker(pipeline_element: Dict) -> Any:
        logging.debug(f"individual_worker ENTER: {pipeline_element}")
        if "name_list" not in pipeline_element:
            pipeline_element["name_list"] = f"c {os.getpid()} {time.time()}"
        else:
            pipeline_element["name_list"] += f" d {os.getpid()} {time.time()}"
        logging.debug(f"individual_worker EXIT: {os.getpid()}, {pipeline_element}")
        return pipeline_element

    processors = [
        SequentialProcessor([
            Processor(individual_worker, f"worker[{p}.{w}]")
            for w in range(4)
        ])
        for p in range(3)
    ]

    par = ParallelProcessor(processors=processors)

    print(f"main: {os.getpid()}")
    pipeline_element = {"name_list": "input"}
    par.start([pipeline_element], parallel_result_handler, [])

    for done_future, done_processor in Processor.done():
        done_processor.done_handler(done_future)
