import logging

from typing import (
    Any,
    Callable,
    Dict,
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
                         pe: Dict) -> Dict:

    print("queue_result_handler: result_type:", result_type)
    print("queue_result_handler: result:", repr(pe))

    if result_type == ProcessorResultType.Result:
        logging.debug(f"queue_result_handler ENTER: {result_type}, {pe}")
        if "name_list" not in pe:
            pe["name_list"] = f"final.{sender.name}"
        else:
            pe["name_list"] += f" final.{sender.name}"
        logging.debug(f"queue_result_handler EXIT: {sender.name}, {pe}")
        print(f"queue_result_handler EXIT A, returning: {sender.name}, {repr(pe)}")
        return pe
    else:
        print(
            "queue_result_handler: got an exception:",
            repr(pe[0]), "last result was:", repr(pe[1]))
        print(f"queue_result_handler EXIT B, returning: {sender.name}, {repr(pe[0])}")
        return pe[0]


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

    print("queue_exception_worker, sender:", sender)
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
    queue_processor.start(arguments=[pipeline_data],
                          result_processor=queue_result_handler,
                          sequential=True)

    result_store = []
    raised = False
    try:
        for result in Processor.done_all():
            print("result returned from done_all:", result)
            if result is None:
                print("WTF")
            result_store.append(result)

    except ValueError:
        raised = True

    print(result_store)
    assert_true(raised)
    assert_equal(len(result_store), 1)
    assert_equal(result_store[0]["name_list"], "start q.0")
