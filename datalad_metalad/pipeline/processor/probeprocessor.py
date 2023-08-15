"""
A processor with a predicatable, reproducable behavior, which can be used for
performance measurements.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass


from datalad.support.constraints import (
    EnsureFloat,
    EnsureInt,
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
    processor_pid: int
    processor_id: id
    invocation_count: int
    sequence_number: int
    content: JSONType | None
    sub_sequence_number: int

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "processor_pid": self.processor_pid,
            "processor_id": self.processor_id,
            "invocation_count": self.invocation_count,
            "sequence_number": self.sequence_number,
            "content": self.content,
            "sub_sequence_number": self.sub_sequence_number
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
                constraints=EnsureFloat() | EnsureNone()
            ),
            ParameterEntry(
                keyword="count",
                help="Number of results in the result set.",
                optional=True,
                default=1,
                constraints=EnsureInt()
            )
        ]
    )

    def __init__(self,
                 *,
                 delay: float | None = None,
                 count: int = 1
                 ):

        super().__init__()
        self.delay = delay
        self.count = count
        self.invocation_count = 0

    def process(self, pipeline_data: PipelineData) -> PipelineData:

        self.invocation_count += 1

        probe_record_list = pipeline_data.get_result("probe-provider-record")
        if not probe_record_list:
            logger.debug(
                f"Ignoring pipeline data without probe provider record: "
                f"{pipeline_data}")
            return pipeline_data

        if self.delay is not None:
            time.sleep(self.delay)

        pipeline_data.add_result_list(
            "probe-processor-record",
            [
                ProbeProcessorResult(
                    state=ResultState.SUCCESS,
                    processor_pid=os.getpid(),
                    processor_id=id(self),
                    invocation_count=self.invocation_count,
                    sequence_number=probe_record_list[0].sequence_number,
                    content=probe_record_list[0].content,
                    sub_sequence_number=sub_sequence_number
                )
                for sub_sequence_number in range(self.count)
            ]
        )
        return pipeline_data
