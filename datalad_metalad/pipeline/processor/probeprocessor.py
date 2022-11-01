"""
A processor with a predicatable, reproducable behavior, which can be used for
performance measurements.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Dict


from datalad.support.constraints import (
    EnsureFloat,
    EnsureNone,
)

from .base import Processor
from ..documentedinterface import (
    DocumentedInterface,
    ParameterEntry,
)
from ..pipelinedata import (
    PipelineData,
    PipelineResult,
    ResultState,
)
from ...metadatatypes import JSONType


logger = logging.getLogger("datalad.metadata.processor.probeprocessor")


@dataclass
class ProbeProcessorResult(PipelineResult):
    sequence_number: int
    content: JSONType | None

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "sequence_number": self.sequence_number,
            "content": self.content
        }


class ProbeProcessor(Processor):

    interface_documentation = DocumentedInterface(
        """A component that "processes" input from probe-providers in a
           predefined time.""",
        [
            ParameterEntry(
                keyword="delay",
                help="""Time between reception of an input and yielding of a
                        result in seconds or `None`. If set to `None`, a result
                        will be yielded immediately.""",
                optional=True,
                default=None,
                constraints=EnsureFloat() | EnsureNone())
        ]
    )

    def __init__(self,
                 *,
                 delay: float | None = None
                 ):

        super().__init__()
        self.delay = delay
        self.last_result_time = 0

    def process(self, pipeline_data: PipelineData) -> PipelineData:

        probe_record_list = pipeline_data.get_result("probe-provider-record")
        if not probe_record_list:
            logger.debug(
                f"Ignoring pipeline data without probe provider record: "
                f"{pipeline_data}")
            return pipeline_data

        self._ensure_delay()
        pipeline_data.add_result_list(
            "probe-processor-record",
            [
                ProbeProcessorResult(
                    state=ResultState.SUCCESS,
                    sequence_number=probe_record_list[0].sequence_number,
                    content=probe_record_list[0].content
                )
            ]
        )
        return pipeline_data

    def _ensure_delay(self):
        current_time = time.time()
        time_diff = current_time - self.last_result_time
        if time_diff >= self.delay:
            self.last_result_time = current_time
            return
        time.sleep(self.delay - time_diff)
        self.last_result_time = time.time()
