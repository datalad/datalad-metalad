from concurrent.futures import (
    CancelledError,
    Executor,
    Future,
    TimeoutError
)
from typing import (
    Any,
    Callable,
    List,
    Optional,
    cast,
)

from .processor import (
    FutureSet,
    Processor,
    ProcessorResult,
    ProcessorResultType,
    ProcessorState,
)
from datalad_metalad.pipelineelement import PipelineResult


__docformat__ = "restructuredtext"


class ProcessorSequence(Processor):
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

        self.state = ProcessorState.Created
        self.active_index = 0
        self.final_result = None
        self.exception = None

    def _ensure_state(self, expected_state: ProcessorState):
        if self.state != expected_state:
            raise RuntimeError(
                f"expected state {expected_state}, "
                f"but current state is {self.state}")

    def _submit(self, result_object: Any):
        future = self.executor.submit(
            self.processors[self.active_index],
            result_object)
        self.future_set.add_future(future, self)

    def start(self):
        self._ensure_state(ProcessorState.Created)
        self.state = ProcessorState.Running
        self._submit(self.initial_result_object)

    def process_done(self,
                     done_future: Future
                     ) -> Optional[ProcessorResult]:
        """process a done future

        :param done_future: the future that is done
        :return: Pipeline result if the last processor was executed
        :rtype: Optional[PipelineResult]
        """
        self._ensure_state(ProcessorState.Running)
        try:
            result = cast(PipelineResult, done_future.result())
        except CancelledError as exc:
            self.state = ProcessorState.Cancelled
            return ProcessorResult(ProcessorResultType.Cancelled, exc)
        except:
            self.exception = done_future.exception(timeout=0)
            self.final_result = None
            self.state = ProcessorState.Raised
            return ProcessorResult(ProcessorResultType.Exception, self.exception)

        self.active_index += 1
        if self.active_index < len(self.processors):
            self._submit(result)
            return None
        else:
            self.final_result = result
            self.state = ProcessorState.Finished
            return ProcessorResult(ProcessorResultType.Result, self.final_result)
