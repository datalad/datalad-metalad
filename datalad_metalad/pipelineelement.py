from copy import deepcopy
from enum import Enum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class ResultState(Enum):
    SUCCESS = "success"
    FAILURE = "error"
    STOP = "stop"


@dataclass
class PipelineResult:
    state: ResultState
    base_error: Optional[Dict] = field(init=False)

    def __post_init__(self):
        self.base_error = None


class PipelineElement:
    def __init__(self):
        self._dynamic = dict()
        self._input: Optional[PipelineResult] = None
        self._result: List[PipelineResult] = []

    def get_dynamic_data(self, key: str, default=None) -> Any:
        return self._dynamic.get(key, default)

    def set_dynamic_data(self, key: str, data: Any):
        self._dynamic[key] = data

    def set_input(self, pipeline_result: PipelineResult):
        self._input = pipeline_result

    def get_input(self) -> PipelineResult:
        return self._input

    def get_results(self) -> List[PipelineResult]:
        return self._result

    def set_results(self, result: List[PipelineResult]):
        self._result = result

    def copy(self) -> "PipelineElement":
        new_pipeline_element = PipelineElement()
        new_pipeline_element._dynamic = deepcopy(self._dynamic)
        return new_pipeline_element
