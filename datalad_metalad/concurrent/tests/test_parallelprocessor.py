import os
import time
import logging
from typing import (
    Any,
    Dict,
    List,
)

from nose.tools import (
    assert_equal,
    assert_true,
)


from datalad_metalad.concurrent.processor import (
    Processor,
    ProcessorResultType,
)
from datalad_metalad.concurrent.parallelprocessor import ParallelProcessor
from datalad_metalad.concurrent.sequentialprocessor import SequentialProcessor


def parallel_result_handler(
        result_type: ProcessorResultType,
        pe: Dict,
        result_store: List):

    logging.debug(f"parallel_result_handler ENTER: {result_type}, {pe}")
    if "name_list" not in pe:
        pe["name_list"] = f"par-{os.getpid()}-{time.time()}"
    else:
        pe["name_list"] += f" par-{os.getpid()}-{time.time()}"
    logging.debug(f"parallel_result_handler EXIT: {os.getpid()}, {pe}")
    print(f"FINAL {pe}")
    result_store.append(pe)


def individual_worker(pipeline_element: Dict) -> Any:
    logging.debug(f"individual_worker ENTER: {pipeline_element}")
    if "name_list" not in pipeline_element:
        pipeline_element["name_list"] = f"ind-{os.getpid()}-{time.time()}"
    else:
        pipeline_element["name_list"] += f" ind-{os.getpid()}-{time.time()}"
    logging.debug(f"individual_worker EXIT: {os.getpid()}, {pipeline_element}")
    return pipeline_element


def test_sp_par_basics():

    processors = [
        SequentialProcessor([
            Processor(individual_worker, f"worker[{p}.{w}]")
            for w in range(4)
        ])
        for p in range(3)
    ]

    par = ParallelProcessor(processors=processors)

    result_store = list()

    print(f"main: {os.getpid()}")
    pipeline_element = {"name_list": "input"}
    par.start(arguments=[pipeline_element],
              result_processor=parallel_result_handler,
              result_processor_args=[result_store],
              sequential=False)

    for done_future, done_processor in Processor.done():
        done_processor.done_handler(done_future)

    assert_equal(len(result_store), 3)
    assert_true(all([
        len(e["name_list"].split()) == 6
        for e in result_store
    ]))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    test_sp_par_basics()
