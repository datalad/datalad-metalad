from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple


class ResultState(Enum):
    SUCCESS = "success"
    FAILURE = "error"
    STOP = "stop"


class PipelineDataState(Enum):
    CONTINUE = "continue"
    STOP = "stop"


@dataclass
class PipelineResult:
    state: ResultState
    base_error: Optional[Dict] = field(init=False)

    def __post_init__(self):
        self.message = ""
        self.base_error = None

    def to_json(self) -> Dict:
        result = dict(state=self.state.name)
        if self.base_error is not None:
            result["error"] = self.base_error
        if self.message:
            result["message"] = self.message
        return result


class PipelineData:
    def __init__(self,
                 initial_result: Optional[Iterable[Tuple[str, List[PipelineResult]]]] = None):

        self._result: Dict[str, List[PipelineResult]] = dict(initial_result or ())
        self._dynamic = dict()
        self.state = PipelineDataState.CONTINUE

    def __eq__(self, other):
        return self._result == other._result \
                and self._dynamic == other._dynamic \
                and self.state == other.state

    def get_dynamic_data(self, key: str, default=None) -> Any:
        return self._dynamic.get(key, default)

    def set_dynamic_data(self, key: str, data: Any):
        self._dynamic[key] = data

    def add_result(self, result_type: str, result: PipelineResult):
        if result_type not in self._result:
            self._result[result_type] = []
        self._result[result_type].append(result)

    def add_result_list(self, result_type: str, results: List[PipelineResult]):
        if result_type not in self._result:
            self._result[result_type] = []
        self._result[result_type].extend(results)

    def set_result(self, result_type: str, result_list: List[PipelineResult]):
        self._result[result_type] = result_list

    def get_result(self, result_type: str) -> Optional[List[PipelineResult]]:
        return self._result.get(result_type, None)

    def copy(self) -> "PipelineData":
        new_pipeline_data = PipelineData()
        new_pipeline_data._dynamic = deepcopy(self._dynamic)
        new_pipeline_data._result = deepcopy(self._result)
        return new_pipeline_data

    def __str__(self):
        return str({
            "type": "PipelineData",
            "state": self.state.name,
            "result": self._result
        })

    def to_json(self) -> Dict:
        json_obj = {
            "state": self.state.name,
            "result": {
                key: [
                    result.to_json()
                    for result in value
                ]
                for key, value in self._result.items()
                if key not in ("path",)
            }
        }
        json_obj["result"]["path"] = str(self._result["path"])
        return json_obj
