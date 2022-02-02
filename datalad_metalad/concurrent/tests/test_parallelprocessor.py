import logging
from typing import (
    Any,
    Dict,
    List,
)

from nose.tools import assert_equal


from ..processor import (
    InterfaceExtendedProcessor,
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


def individual_worker(sender, pipeline_data: Dict) -> Any:
    if "name_list" not in pipeline_data:
        pipeline_data["name_list"] = f"i.{sender.name}"
    else:
        pipeline_data["name_list"] += f" i.{sender.name}"
    return pipeline_data


def test_sp_par_basics():

    processors = [
        QueueProcessor([
            InterfaceExtendedProcessor(individual_worker, f"{p}.{w}")
            for w in range(4)
        ])
        for p in range(3)
    ]

    par = ParallelProcessor(processors=processors)

    result_store = list()

    pipeline_data = {"name_list": "start"}
    par.start(arguments=[pipeline_data],
              result_processor=parallel_result_handler,
              result_processor_args=[result_store],
              sequential=False)

    Processor.done_all()

    assert_equal(len(result_store), 3)
    patterns = [
        f"start i.{p}.{0} i.{p}.{1} i.{p}.{2} i.{p}.{3} p.{p}.3"
        for p in range(len(result_store))
    ]
    results = [result["name_list"] for result in result_store]

    # Compare the sorted results, because the individual parallel
    # queues might terminate in any order.
    assert_equal(sorted(patterns), sorted(results))
