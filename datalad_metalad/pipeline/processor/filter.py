"""
Extract metadata and add it to a conduct-element.
"""
import logging
from dataclasses import dataclass, field
from typing import (
    Dict,
    Optional,
)

from .base import Processor
from ..documentedinterface import (
    DocumentedInterface,
    ParameterEntry,
)
from ..pipelinedata import (
    PipelineData,
    PipelineResult,
)


logger = logging.getLogger("datalad.metadata.processor.filter")


@dataclass
class MetadataFilterResult(PipelineResult):
    path: str
    context: Optional[Dict] = None
    metadata_record: Optional[Dict] = field(init=False)

    def to_json(self) -> Dict:
        return {
            **super().to_json(),
            "path": str(self.path),
            "metadata_record": self.metadata_record
        }


class MetadataFilter(Processor):

    interface_documentation = DocumentedInterface(
        "A component that extracts metadata by running an extractor.",
        [
            ParameterEntry(
                keyword="filter_name",
                help="The name of the filter that should be executed.",
                optional=False),
        ]
    )

    def __init__(self,
                 *,
                 filter_name: str
                 ):

        self.filter_name = filter_name

    def process(self, pipeline_data: PipelineData) -> PipelineData:

        pass
