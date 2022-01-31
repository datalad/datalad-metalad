import logging
from nose.tools import assert_equal
from typing import (
    Any,
    Dict,
    List,
)

from ..processor import (
    Processor,
    ProcessorResultType,
)
from ..sequentialprocessor import SequentialProcessor


def sequential_result_handler(sender: Processor,
                              result_type: ProcessorResultType,
                              pe: Dict,
                              result_store: List):

    logging.debug(f"sequential_result_handler ENTER: {result_type}, {pe}")
    if "name_list" not in pe:
        pe["name_list"] = f"final.{sender.name}"
    else:
        pe["name_list"] += f" final.{sender.name}"
    logging.debug(f"parallel_result_handler EXIT: {sender.name}, {pe}")
    result_store.append(pe)


def sequential_worker(sender: Processor,
                      pipeline_element: Dict) -> Any:

    if "name_list" not in pipeline_element:
        pipeline_element["name_list"] = f"s.{sender.name}"
    else:
        pipeline_element["name_list"] += f" s.{sender.name}"
    return pipeline_element


def test_basics():
    processors = [
        Processor(a_callable=sequential_worker, name=f"{i}")
        for i in range(4)
    ]

    seq = SequentialProcessor(
        processors=processors,
        name=f""
    )

    result_store = list()

    pipeline_element = {"name_list": "start"}
    seq.start(arguments=[pipeline_element],
              result_processor=sequential_result_handler,
              result_processor_args=[result_store],
              sequential=False)

    for done_future, done_processor in Processor.done():
        done_processor.done_handler(done_future)

    assert_equal(
        result_store[0]["name_list"],
        "start s.0 s.1 s.2 s.3 final.3"
    )
