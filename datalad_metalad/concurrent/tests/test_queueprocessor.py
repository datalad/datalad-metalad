import logging
import sys

from nose.tools import assert_equal
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
)

from ..processor import (
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


class IWorker(Processor):
    def __init__(self,
                 wrapped_callable: Callable,
                 name: Optional[str] = None):

        Processor.__init__(self, self.worker_interface, name)
        self.wrapped_callable = wrapped_callable

    def __repr__(self):
        return f"IWorker[{self.name}]"

    def worker_interface(self, *args, **kwargs) -> Any:
        return self.wrapped_callable(self, *args, **kwargs)


def queue_worker(sender, pipeline_element: Dict) -> Any:
    if "name_list" not in pipeline_element:
        pipeline_element["name_list"] = f"q.{sender.name}"
    else:
        pipeline_element["name_list"] += f" q.{sender.name}"
    return pipeline_element


def test_basics():

    processors = [
        IWorker(queue_worker, f"{i}")
        for i in range(4)
    ]

    queue_processor = QueueProcessor(processors=processors, name=f"")

    result_store = list()

    pipeline_element = {"name_list": "start"}
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
    processors = [
        IWorker(queue_exception_worker, f"{i}")
        for i in range(4)
    ]

    queue_processor = QueueProcessor(
        processors=processors,
        name=f""
    )

    result_store = list()

    pipeline_element = {"name_list": "start"}
    queue_processor.start(arguments=[pipeline_element],
                          result_processor=queue_result_handler,
                          result_processor_args=[result_store],
                          sequential=False)

    Processor.done_all()

    assert_equal(
        result_store[0]["name_list"],
        "start q.0"
    )
