import json
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

from dataladmetadatamodel.metadatapath import MetadataPath

from ..add import BatchAdder
from ...pipelinedata import (
    PipelineData,
    ResultState,
)
from ...processor.extract import MetadataExtractorResult
from ...provider.datasettraverse import DatasetTraverseResult
from ....metadatatypes.metadata import (
    AggregationInfo,
    MetadataRecord,
)


def test_batch_adder_basic():

    test_record = DatasetTraverseResult(
        state=ResultState.SUCCESS,
        fs_base_path=Path("/tmp/a"),
        type="both",
        dataset_path=Path(""),
        dataset_id="0",
        dataset_version="0"
    )

    metadata_extractor_result = MetadataExtractorResult(
        state=ResultState.SUCCESS,
        path="/tmp/a")

    metadata_extractor_result.metadata_record = MetadataRecord(
        type="dataset",
        dataset_id=UUID(int=1),
        dataset_version="2",
        extractor_name="dummy-extractor",
        extractor_version="0.nothing",
        extraction_parameter={},
        extraction_time=1.0,
        agent_name="Dummy Name",
        agent_email="dummy@example.com",
        extracted_metadata={"some": "data"},
        aggregation_info=AggregationInfo(
            root_dataset_id=UUID(int=2),
            root_dataset_version="9",
            dataset_path=MetadataPath("a"),
        )
    )
    pipeline_data = PipelineData()
    pipeline_data.add_result("dataset-traversal-record", test_record)
    pipeline_data.add_result("metadata", metadata_extractor_result)

    class BatchCommandMock:
        def close(self):
            pass

        def __call__(self, *args, **kwargs):
            return json.dumps({"status": "ok"})

    with patch("datalad_metalad.pipeline.consumer.add.BatchedCommand") as bc:
        bc.return_value = BatchCommandMock()
        batch_adder = BatchAdder(dataset="/tmp/a", aggregate=False)
        batch_adder.consume(pipeline_data)

        batch_adder = BatchAdder(dataset="/tmp/a", aggregate=True)
        batch_adder.consume(pipeline_data)
