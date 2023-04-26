"""
A provider with a controlled, reproducible data generation behavior. It can be
used for performance measurements.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import (
    Dict,
    Iterable,
)

from datalad.support.constraints import (
    EnsureFloat,
    EnsureInt,
    EnsureNone,
)

from .base import Provider
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


lgr = logging.getLogger('datalad.metadata.pipeline.provider.probeprovider')

default_mapper_family = "git"


@dataclass
class ProbeProviderResult(PipelineResult):
    provider_pid: int
    provider_id: id
    sequence_number: int
    content: JSONType | None

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "provider_pid": self.provider_pid,
            "provider_id": self.provider_id,
            "sequence_number": self.sequence_number,
            "content": self.content
        }


class ProbeProvider(Provider):

    interface_documentation = DocumentedInterface(
        """A component that emits predefined data in a specific rate.""",
        [
            ParameterEntry(
                keyword="count",
                help="Number of results that should be delivered.",
                optional=False,
                constraints=EnsureInt()),
            ParameterEntry(
                keyword="delay",
                help="""Time between two consecutive results in seconds or
                        `None`. If set to `None`, results will be yielded as
                        fast as possible.""",
                optional=True,
                default=None,
                constraints=EnsureFloat() | EnsureNone()),
            ParameterEntry(
                keyword="content",
                help="""Content that will be returned in
                        `ProbeProviderResult.content`.""",
                optional=True,
                default=None)
        ]
    )

    def __init__(self,
                 *,
                 count: int,
                 delay: float | None = None,
                 content: JSONType | None = None
                 ):

        super().__init__()

        self.count = count
        self.delay = delay
        self.content = content

        self.current_count = 0
        self.last_result_time = 0

    def _create_result(self,
                       count: int
                       ) -> PipelineData:

        return PipelineData((
            ("path", f"probe:{count}"),
            (
                "probe-provider-record",
                [
                    ProbeProviderResult(
                        state=ResultState.SUCCESS,
                        provider_pid=os.getpid(),
                        provider_id=id(self),
                        sequence_number=count,
                        content=self.content
                    )
                ]
            )
        ))

    def _ensure_delay(self):
        current_time = time.time()
        time_diff = current_time - self.last_result_time
        if time_diff >= self.delay:
            self.last_result_time = current_time
            return
        time.sleep(self.delay - time_diff)
        self.last_result_time = time.time()

    def next_object(self) -> Iterable:
        while self.current_count < self.count:
            if self.delay is not None:
                self._ensure_delay()
            yield self._create_result(self.current_count)
            self.current_count += 1
