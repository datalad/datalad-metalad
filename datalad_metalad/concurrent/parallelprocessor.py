import deepcopy
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
    ProcessorState,
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

        self.state = ProcessorState.Created

    def _ensure_state(self, expected_state: ProcessorState):
        if self.state != expected_state:
            raise RuntimeError(
                f"expected state {expected_state}, "
                f"but current state is {self.state}")

    def _submit(self, proc: Callable, result_object: Any):
        future = self.executor.submit(proc, result_object)
        self.future_set.add_future(future, self)

    def start(self):
        self._ensure_state(ProcessorState.Created)
        self.state = ProcessorState.Running
        for proc in self.processors:
            proc_input = deepcopy(self.initial_result_object)
            self._submit(proc, proc_input)

    def process_done(self,
                     done_future: Future
                     ) -> Any:
        """process a done future

        :param done_future: the future that is done
        :return: Pipeline result if the last processor was executed
        :rtype: Optional[PipelineResult]
        """
        self._ensure_state(ProcessorState.Running)
        if done_future != self.active_future:
            raise RuntimeError("done_future != self.active_future")
        try:
            result = cast(PipelineResult, done_future.result())
        except TimeoutError:
            return None
        except CancelledError:
            self.state = ProcessorState.Cancelled
            return None
        except:
            self.exception = self.active_future.exception(timeout=0)
            self.final_result = None
            self.state = ProcessorState.Raised
            return None

        self.active_index += 1
        if self.active_index < len(self.processors):
            self._submit(result)
            return None
        else:
            self.final_result = result
            self.state = ProcessorState.Finished
            return self.final_result
