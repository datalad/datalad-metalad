import logging
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from typing import (
    Dict,
    Optional,
    cast,
)

from datalad.support.constraints import EnsureBool
from datalad.runner.coreprotocols import NoCapture
from datalad.runner.protocol import GeneratorMixIn
from datalad.runner.runner import WitlessRunner


from .base import Consumer
from ..documentedinterface import (
    DocumentedInterface,
    ParameterEntry,
)
from ..pipelinedata import (
    PipelineData,
    PipelineDataState,
    PipelineResult,
    ResultState,
)
from ..processor.extract import MetadataExtractorResult
from ..provider.datasettraverse import DatasetTraverseResult
from ..provider.metadatatraverse import MetadataTraverseResult


logger = logging.getLogger("datalad.meta-conduct.consumer.add")


@dataclass
class MetadataStdinAddResult(PipelineResult):
    path: str

    def to_dict(self) -> Dict:
        return {
            **super().to_dict(),
            "path": str(self.path)
        }


class GeneratorNoCapture(NoCapture, GeneratorMixIn):
    pass


class StdinAdder(Consumer):

    interface_documentation = DocumentedInterface(
        "A component that adds metadata to a dataset that is read from stdin",
        [
            ParameterEntry(
                keyword="dataset",
                help="""A path to the dataset in which the metadata should be
                        stored.""",
                optional=True,
                default="."),
            ParameterEntry(
                keyword="aggregate",
                help="""A boolean that indicates whether sub-dataset metadata
                        should be added into the root-dataset, i.e. aggregated
                        (aggregate=True), or whether sub-dataset metadata should
                        be ignored (aggregate=False).""",
                optional=True,
                default=True,
                constraints=EnsureBool())
        ]
    )

    def __init__(self,
                 *,
                 dataset: str = ".",
                 aggregate: Optional[bool] = True):

        self.aggregate = aggregate
        self.input_queue = Queue()
        self.runner = WitlessRunner()
        self.runner.run(
            cmd=["datalad", "meta-add", "-d", dataset, "--json-lines", "-i", "-"],
            protocol=GeneratorNoCapture,
            stdin=self.input_queue
        )

    def consume(self, pipeline_data: PipelineData) -> PipelineData:

        if pipeline_data.state == PipelineDataState.STOP:
            self.input_queue.put(None)

        metadata_result_list = pipeline_data.get_result("metadata")
        if not metadata_result_list:
            logger.debug(
                f"Ignoring pipeline data without metadata: "
                f"{pipeline_data}")
            return pipeline_data

        # If aggregate is specified, we aggregate sub-dataset metadata into this
        # metadata store.
        additional_values = get_dataset_traverse_root(pipeline_data)
        if additional_values:
            if not self.aggregate:
                logger.debug(
                    "ignoring non-root metadata because aggregate is not set")
                return pipeline_data
        else:
            additional_values = get_metadata_traverse_root(pipeline_data)

        for metadata_extractor_result in metadata_result_list:

            metadata_record = cast(
                MetadataExtractorResult,
                metadata_extractor_result).metadata_record

            metadata_record["dataset_id"] = str(metadata_record["dataset_id"])
            if "path" in metadata_record:
                metadata_record["path"] = str(metadata_record["path"])
                path = metadata_record["path"]
            else:
                path = ""

            self.input_queue.put({
                **metadata_record,
                **(additional_values or {})
            })

            add_result = MetadataStdinAddResult(ResultState.SUCCESS, path)
            pipeline_data.set_result("path", path)
            pipeline_data.add_result_list("batch_add", [add_result])

        return pipeline_data


def get_dataset_traverse_root(pipeline_data: PipelineData) -> Optional[Dict]:

    dataset_traversal_record = pipeline_data.get_result("dataset-traversal-record")
    if dataset_traversal_record is None:
        return None

    dataset_traversal_record = cast(
        DatasetTraverseResult,
        dataset_traversal_record[0])

    if dataset_traversal_record.dataset_path == Path(""):
        return {}
    return {
        "dataset_path": str(dataset_traversal_record.dataset_path),
        "root_dataset_id": str(dataset_traversal_record.root_dataset_id),
        "root_dataset_version": str(dataset_traversal_record.root_dataset_version)
    }


def get_metadata_traverse_root(pipeline_data: PipelineData) -> Optional[Dict]:

    # MetadataRecord traverse only yields metadata of one metadata-store. There
    # is no need and no possibility for further aggregation, but some records
    # might already be aggregated. They have to be added as aggregated records.

    metadata_traversal_record = pipeline_data.get_result("metadata-traversal-record")
    if metadata_traversal_record is None:
        return None

    metadata_traversal_record = cast(
        MetadataTraverseResult,
        metadata_traversal_record[0])

    metadata_record = metadata_traversal_record.metadata_record
    if all((metadata_record["dataset_path"],
            metadata_record["root_dataset_id"],
            metadata_record["root_dataset_version"])):
        return {
            "dataset_path": str(metadata_record["dataset_path"]),
            "root_dataset_id": str(metadata_record["root_dataset_id"]),
            "root_dataset_version": str(metadata_record["root_dataset_version"])
        }
    return {}
