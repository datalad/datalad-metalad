from uuid import (
    UUID,
    uuid4,
)

from dataladmetadatamodel.metadata import ExtractorConfiguration
from dataladmetadatamodel.metadatapath import MetadataPath

from ..metadata import (
    MetadataRecord,
)


random_uuid = uuid4()

test_json_1 = dict(
    type="file",
    extractor_id=UUID(int=3),
    extractor_name="ext-a",
    extractor_version="1.0",
    extraction_parameter=ExtractorConfiguration(
        version="1.0",
        parameter={"p1": "v1", "p2": "v2"}
    ).to_json_obj(),
    extraction_time=1233.444,
    agent_name="agent_a",
    agent_email="agent_a@example.com",
    dataset_id=str(random_uuid),
    dataset_version="001122334455776660909090",
    extracted_metadata={"some": "metadata"},
    path=str(MetadataPath("d1/file_1.txt"))
)


def test_basic():
    md = MetadataRecord(
        type="file",
        extractor_id=UUID(int=4),
        extractor_name="ext-a",
        extractor_version="1.0",
        extraction_parameter={"p1": "v1", "p2": "v2"},
        extraction_time=1233.444,
        agent_name="agent_a",
        agent_email="agent_a@example.com",
        dataset_id=random_uuid,
        dataset_version="001122334455776660909090",
        extracted_metadata={"some": "metadata"},
        path=MetadataPath("d1/file_1.txt")
    )

    md.as_json_obj()
    md.as_json_str()


def test_uuid_conversion():
    md = MetadataRecord(
        type="file",
        extractor_id=UUID(int=5),
        extractor_name="ext-a",
        extractor_version="1.0",
        extraction_parameter={"p1": "v1", "p2": "v2"},
        extraction_time=1233.444,
        agent_name="agent_a",
        agent_email="agent_a@example.com",
        dataset_id=random_uuid,
        dataset_version="001122334455776660909090",
        extracted_metadata={"some": "metadata"},
        path=MetadataPath("d1/file_1.txt")
    )

    md.as_json_obj()
    md.as_json_str()


def test_from_json():
    md = MetadataRecord.from_json(test_json_1)
