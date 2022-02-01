import logging
from typing import (
    Any,
    Dict,
    List,
)

from nose.tools import assert_equal


from ..processor import (
    Processor,
    ProcessorResultType,
)
from ..parallelprocessor import ParallelProcessor
from ..queueprocessor import QueueProcessor


def parallel_result_handler(sender: Processor,
                            result_type: ProcessorResultType,
                            pe: Dict,
                            result_store: List):

    logging.debug(f"parallel_result_handler ENTER: {result_type}, {pe}")
    if "name_list" not in pe:
        pe["name_list"] = f"p.{sender.name}"
    else:
        pe["name_list"] += f" p.{sender.name}"
    logging.debug(f"parallel_result_handler EXIT: {sender.name}, {pe}")
    result_store.append(pe)


def individual_worker(sender: Processor,
                      pipeline_element: Dict) -> Any:

    logging.debug(f"individual_worker ENTER: {pipeline_element}")
    if "name_list" not in pipeline_element:
        pipeline_element["name_list"] = f"i.{sender.name}"
    else:
        pipeline_element["name_list"] += f" i.{sender.name}"
    logging.debug(f"individual_worker EXIT: {sender.name}, {pipeline_element}")
    return pipeline_element


def test_sp_par_basics():

    processors = [
        QueueProcessor([
            (Processor, [individual_worker, f"{p}.{w}"], dict())
            for w in range(4)
        ])
        for p in range(3)
    ]

    par = ParallelProcessor(processors=processors)

    result_store = list()

    pipeline_element = {"name_list": "start"}
    par.start(arguments=[pipeline_element],
              result_processor=parallel_result_handler,
              result_processor_args=[result_store],
              sequential=False)

    for done_future, done_processor in Processor.done():
        done_processor.done_handler(done_future)

    assert_equal(len(result_store), 3)
    patterns = [
        f"start i.{p}.{0} i.{p}.{1} i.{p}.{2} i.{p}.{3} p.{p}.3"
        for p in range(len(result_store))
    ]
    results = [result["name_list"] for result in result_store]

    # Compare the sorted results, because the individual parallel
    # queues might terminate in any order.
    assert_equal(sorted(patterns), sorted(results))