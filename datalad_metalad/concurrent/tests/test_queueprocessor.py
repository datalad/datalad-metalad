import logging

from nose.tools import assert_equal
from typing import (
    Any,
    Callable,
    Dict,
    List,
)

from ..processor import (
    InterfaceExtendedProcessor,
    Processor,
    ProcessorResultType,
)
from ..queueprocessor import QueueProcessor


def queue_result_handler(sender: QueueProcessor,
                         result_type: ProcessorResultType,
                         pe: Dict,
                         result_store: List):

    if result_type == ProcessorResultType.Result:
        logging.debug(f"queue_result_handler ENTER: {result_type}, {pe}")
        if "name_list" not in pe:
            pe["name_list"] = f"final.{sender.name}"
        else:
            pe["name_list"] += f" final.{sender.name}"
        logging.debug(f"queue_result_handler EXIT: {sender.name}, {pe}")
    result_store.append(pe)


def queue_worker(sender, pipeline_element: Dict) -> Any:
    if "name_list" not in pipeline_element:
        pipeline_element["name_list"] = f"q.{sender.name}"
    else:
        pipeline_element["name_list"] += f" q.{sender.name}"
    return pipeline_element


def _get_queue_processor(worker: Callable, count: int) -> QueueProcessor:
    processors = [
        InterfaceExtendedProcessor(worker, f"{i}")
        for i in range(count)
    ]
    return QueueProcessor(processors=processors, name=f"")


def test_basics():

    result_store = list()

    pipeline_element = {"name_list": "start"}
    queue_processor = _get_queue_processor(queue_worker, 4)
    queue_processor.start(arguments=[pipeline_element],
                          result_processor=queue_result_handler,
                          result_processor_args=[result_store],
                          sequential=False)

    Processor.done_all()

    assert_equal(
        result_store[0]["name_list"],
        "start q.0 q.1 q.2 q.3 final.3"
    )


def queue_exception_worker(sender: Processor,
                           pipeline_element: Dict) -> Any:
    if sender.name == "1":
        raise ValueError("Sender' name is '1'")

    if "name_list" not in pipeline_element:
        pipeline_element["name_list"] = f"q.{sender.name}"
    else:
        pipeline_element["name_list"] += f" q.{sender.name}"
    return pipeline_element


def test_exception():

    result_store = list()

    pipeline_element = {"name_list": "start"}
    queue_processor = _get_queue_processor(queue_exception_worker, 4)
    queue_processor.start(arguments=[pipeline_element],
                          result_processor=queue_result_handler,
                          result_processor_args=[result_store],
                          sequential=False)

    Processor.done_all()

    assert_equal(
        result_store[0]["name_list"],
        "start q.0"
    )
