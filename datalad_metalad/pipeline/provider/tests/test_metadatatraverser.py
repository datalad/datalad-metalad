from pathlib import Path
from unittest.mock import patch

from datalad.tests.utils import assert_equal

from ..metadatatraverse import (
    MetadataTraverseResult,
    MetadataTraverser,
    ResultState,
)
from ...pipelinedata import PipelineData


def test_metadata_traverser():

    test_metadata_store = "abc"
    test_pattern = "*:*"
    test_recursive = True
    test_record = {
        "status": "ok",
        "some": "key"
    }

    def meta_dump_mock(dataset, path, recursive):
        assert_equal(dataset, Path(test_metadata_store))
        assert_equal(path, test_pattern)
        assert_equal(test_recursive, recursive)
        yield test_record

    traverser = MetadataTraverser(
        metadata_store=test_metadata_store,
        pattern=test_pattern,
        recursive=test_recursive)

    expected = PipelineData((
        ("path", Path(test_metadata_store)),
        (
            "metadata-traversal-record",
            [
                MetadataTraverseResult(
                    state=ResultState.SUCCESS,
                    metadata_store=Path(test_metadata_store),
                    metadata_record=test_record)
            ]
        )
    ))

    with patch("datalad_metalad.pipeline.provider.metadatatraverse.meta_dump") as md:
        md.side_effect = meta_dump_mock
        result = list(traverser.next_object())[0]
        assert_equal(result, expected)
