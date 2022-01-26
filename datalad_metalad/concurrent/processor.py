import concurrent.futures
import dataclasses
import enum
import queue
from typing import (
    Any,
    Union,
)

from datalad_metalad.pipelineelement import PipelineResult


__docformat__ = "restructuredtext"


class ProcessorResultType(enum.Enum):
    Result = 0
    Exception = 1
    Cancelled = 2


@dataclasses.dataclass
class ProcessorResult:
    result_type: ProcessorResultType
    result: Union[Exception, Any]


class ProcessorState(enum.Enum):
    Created = 0
    Running = 1
    Finished = 2
    Raised = 3
    Cancelled = 4


class Processor:
    def __init__(self):
        self.result_queue = queue.Queue()

    def _add_result(self, result: PipelineResult):
        if result is None:
            raise ValueError("None-results are not allowed in processor")
        self.result_queue.put(result)

    def _add_end(self):
        self.result_queue.put(None)


class FutureSet:
    def __init__(self):
        self.futures = dict()

    def add_future(self,
                   future: concurrent.futures.Future,
                   processor: Processor):
        self.futures[future] = processor

    def remove_future(self, future: concurrent.futures.Future):
        del self.futures[future]

    @property
    def done(self):
        while self.futures:
            for future in concurrent.futures.as_completed(self.futures.keys()):
                yield future, self.futures[future]
                self.remove_future(future)
