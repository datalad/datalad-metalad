from copy import deepcopy
from concurrent.futures import (
    CancelledError,
    Executor,
    Future,
)
from typing import (
    Any,
    Callable,
    List,
    Optional,
)

from .processor import (
    FutureSet,
    Processor,
    ProcessorResult,
    ProcessorResultType,
)
from datalad_metalad.pipelineelement import PipelineResult


__docformat__ = "restructuredtext"


class ProcessorParallel(Processor):
    def __init__(self,
                 processors: List[Callable],
                 initial_result_object: Any,
                 executor: Executor,
                 future_set: FutureSet):

        Processor.__init__(self)
        self.processors = processors
        self.initial_result_object = initial_result_object
        self.executor = executor
        self.future_set = future_set

    def _submit(self, proc: Callable, result_object: Any):
        future = self.executor.submit(proc, result_object)
        self.future_set.add_future(future, self)

    def start(self):
        for proc in self.processors:
            proc_input = deepcopy(self.initial_result_object)
            self._submit(proc, proc_input)

    def process_done(self, done_future: Future) -> Any:
        """process a done future

        :param done_future: the future that is done
        :return: Pipeline result if the last processor was executed
        :rtype: Optional[PipelineResult]
        """
        try:
            result = done_future.result()
            return ProcessorResult(ProcessorResultType.Result, result)
        except CancelledError as exc:
            return ProcessorResult(ProcessorResultType.Cancelled, exc)
        except:
            exc = done_future.exception(timeout=0)
            return ProcessorResult(ProcessorResultType.Exception, exc)
