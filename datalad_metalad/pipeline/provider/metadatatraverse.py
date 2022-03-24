"""
Traversal of metadata.
"""
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Dict,
    Iterable,
    Optional,
)

from datalad.api import meta_dump
from datalad.support.constraints import EnsureBool

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


lgr = logging.getLogger('datalad.metadata.pipeline.provider.metadatatraverse')

default_mapper_family = "git"


@dataclass
class MetadataTraverseResult(PipelineResult):
    metadata_store: Path
    metadata_record: JSONType

    message: str = ""


class MetadataTraverser(Provider):

    interface_documentation = DocumentedInterface(
        """A component that traverses a metadata store and generates
           metadata-data for every metadata entry.""",
        [
            ParameterEntry(
                keyword="metadata_store",
                help="""Path to the git repository in which metadata is 
                        stored.""",
                optional=False),
            ParameterEntry(
                keyword="pattern",
                help="""MetadataRecord path pattern that is used to identify entry
                        points. If not path is given, "." is used, i.e. the
                        root metadata entry is listed.""",
                optional=True,
                default="."),
            ParameterEntry(
                keyword="recursive",
                help="""If set to True, list all sub entries recursively.""",
                optional=True,
                default=False,
                constraints=EnsureBool())
        ]
    )

    def __init__(self,
                 *,
                 metadata_store: str,
                 pattern: Optional[str] = ".",
                 recursive: bool = False
                 ):

        self.metadata_store = Path(metadata_store)
        self.pattern = pattern
        self.recursive = recursive

    def _create_result(self,
                       state: ResultState,
                       record: Dict,
                       message: str = ""
                       ) -> PipelineData:

        return PipelineData((
            ("path", self.metadata_store),
            (
                "metadata-traversal-record",
                [
                    MetadataTraverseResult(**{
                        "state": state,
                        "metadata_store": self.metadata_store,
                        "metadata_record": record,
                        "message": message
                    })
                ]
            )))

    def _traverse_metadata(self) -> Iterable:

        for result in meta_dump(dataset=self.metadata_store,
                                path=self.pattern,
                                recursive=self.recursive):

            if result["status"] == "ok":
                yield self._create_result(
                    state=ResultState.SUCCESS,
                    record=result)
            else:
                yield self._create_result(
                    state=ResultState.FAILURE,
                    record=result,
                    message=result["message"])

    def next_object(self) -> Iterable:
        yield from self._traverse_metadata()
