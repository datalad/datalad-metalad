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
from ..queueprocessor import QueueProcessor


def queue_result_handler(sender: Processor,
                         result_type: ProcessorResultType,
                         pe: Dict,
                         result_store: List):

    logging.debug(f"queue_result_handler ENTER: {result_type}, {pe}")
    if "name_list" not in pe:
        pe["name_list"] = f"final.{sender.name}"
    else:
        pe["name_list"] += f" final.{sender.name}"
    logging.debug(f"queue_result_handler EXIT: {sender.name}, {pe}")
    result_store.append(pe)


def queue_worker(sender: Processor,
                 pipeline_element: Dict) -> Any:

    if "name_list" not in pipeline_element:
        pipeline_element["name_list"] = f"q.{sender.name}"
    else:
        pipeline_element["name_list"] += f" q.{sender.name}"
    return pipeline_element


def test_basics():
    processor_factories = [
        (Processor, [queue_worker], dict(name=f"{i}"))
        for i in range(4)
    ]

    queue_processor = QueueProcessor(
        processor_factories=processor_factories,
        name=f""
    )

    result_store = list()

    pipeline_element = {"name_list": "start"}
    queue_processor.start(arguments=[pipeline_element],
                          result_processor=queue_result_handler,
                          result_processor_args=[result_store],
                          sequential=False)

    for done_future, done_processor in Processor.done():
        done_processor.done_handler(done_future)

    assert_equal(
        result_store[0]["name_list"],
        "start q.0 q.1 q.2 q.3 final.3"
    )
