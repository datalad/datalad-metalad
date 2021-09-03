import logging
from copy import deepcopy
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Dict, List, Optional


logger = logging.getLogger("datalad.metadata.pipeline")


def _get_object_from_name(name: str):
    if ":" in name:
        module_name, object_name = name.split(":")
    else:
        module_name, object_name = name, None

    module = import_module(module_name)
    if object_name:
        return getattr(module, object_name)
    return module


class PipelineContext:
    def __init__(self,
                 initial_positional_arguments: Optional[List[Any]] = None,
                 initial_keyword_arguments: Optional[Dict[str, Any]] = None):

        self.positional_arguments = initial_positional_arguments or []
        self.keyword_arguments = initial_keyword_arguments or dict()
        self._results: Dict[str, Dict[str, Any]] = dict()

    def set_results(self, element_name: str, result: Dict[str, Any]):
        self._results[element_name] = result

    def unset_result(self, element_name: str):
        if element_name not in self._results:
            logger.warning(f"Attempting to remove non-existing result: {element_name}")
        del self._results[element_name]

    def has_result(self, element_name: str) -> bool:
        return element_name in self._results

    def get_result(self, element_name: str) -> Dict[str, Any]:
        return self._results.get(element_name, None)

    def copy(self) -> "PipelineContext":
        new_pipeline_context = PipelineContext()
        new_pipeline_context._results = deepcopy(self._results)
        return new_pipeline_context

    def __str__(self):
        return str({
            "type": "PipelineContext",
            "results": self._results
        })


@dataclass
class ArgumentMapping:
    source_element_name: str
    result_field: str
    custom_mapper: Optional[str]

    def __post_init__(self):
        if self.custom_mapper is None:
            return
        self.custom_mapper_callable = _get_object_from_name(self.custom_mapper)


@dataclass
class PositionalArgumentMapping(ArgumentMapping):
    position: int


@dataclass
class KeywordArgumentMapping(ArgumentMapping):
    required: bool
    prefix: str
    append_result: bool


@dataclass
class PipelineElementDefinition:
    name: str
    callable_name: str

    def __post_init__(self):
        self.callable = _get_object_from_name(self.callable_name)


@dataclass
class PipelineDefinition:
    elements: List[PipelineElementDefinition]



class SequentialExecutor:
    def __init__(self):