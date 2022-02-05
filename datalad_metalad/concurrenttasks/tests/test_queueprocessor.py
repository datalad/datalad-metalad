import logging
import traceback
from typing import (
    Any,
    Callable,
    Dict,
    List,
)

from nose.tools import (
    assert_equal,
    assert_true,
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
                         result_store: List) -> Dict:

    print(f"queue_result_handler({sender}, {result_type}, {pe}, {result_store})")

    if result_type == ProcessorResultType.Result:
        logging.debug(f"queue_result_handler ENTER: {result_type}, {pe}")
        if "name_list" not in pe:
            pe["name_list"] = f"final.{sender.name}"
        else:
            pe["name_list"] += f" final.{sender.name}"
        logging.debug(f"queue_result_handler EXIT: {sender.name}, {pe}")
        print(f"queue_result_handler EXIT A, appending {repr(pe)}")
        result_store.append(pe)
    else:
        print(
            "queue_result_handler: got an exception:",
            repr(pe[0]), "last result was:", repr(pe[1]))
        exc = pe[0]
        traceback.print_exception(type(exc), exc, exc.__traceback__)

        print(f"queue_result_handler EXIT B, appending: {repr(pe[1])}")
        result_store.append(pe[1])


def queue_worker(sender, pipeline_data: Dict) -> Any:
    if "name_list" not in pipeline_data:
        pipeline_data["name_list"] = f"q.{sender.name}"
    else:
        pipeline_data["name_list"] += f" q.{sender.name}"
    return pipeline_data


def _get_queue_processor(worker: Callable, count: int) -> QueueProcessor:
    processors = [
        InterfaceExtendedProcessor(worker, f"{i}")
        for i in range(count)
    ]
    return QueueProcessor(processors=processors, name=f"")


def test_basics():

    pipeline_data = {"name_list": "start"}
    queue_processor = _get_queue_processor(queue_worker, 4)
    queue_processor.start(arguments=[pipeline_data],
                          result_processor=queue_result_handler,
                          sequential=False)

    result_store = []
    for result in Processor.done_all():
        if result is not None:
            result_store.append(result)

    assert_equal(
        result_store[0]["name_list"],
        "start q.0 q.1 q.2 q.3 final.3"
    )


def queue_exception_worker(sender: Processor,
                           pipeline_data: Dict) -> Any:

    print(f"ENTER: queue_exception_worker({sender}, {repr(pipeline_data)})")
    if sender.name == "1":
        print("queue_exception_worker, raising ValueError")
        raise ValueError("Sender' name is '1'")

    if "name_list" not in pipeline_data:
        pipeline_data["name_list"] = f"q.{sender.name}"
    else:
        pipeline_data["name_list"] += f" q.{sender.name}"
    print("queue_exception_worker, pipeline_data:", pipeline_data)
    return pipeline_data


def test_exception():


    pipeline_data = {"name_list": "start"}
    queue_processor = _get_queue_processor(queue_exception_worker, 4)

    result_store = []
    queue_processor.start(arguments=[pipeline_data],
                          result_processor=queue_result_handler,
                          result_processor_args=[result_store],
                          sequential=False)

    raised = False
    try:
        for result in Processor.done_all():
            print("result returned from done_all:", result)
            if result is None:
                print("WTF")

    except ValueError:
        raised = True

    print(result_store)
    assert_true(raised)
    assert_equal(len(result_store), 1)
    assert_equal(result_store[0]["name_list"], "start q.0")
