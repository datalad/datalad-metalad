from typing import List, Optional

from datalad.support.exceptions import (
    InsufficientArgumentsError,
    NoDatasetArgumentFound,
)
from datalad.utils import ensure_unicode


class NoDatasetIdFound(NoDatasetArgumentFound):
    """Raised whenever a dataset ID cannot be found in a dataset."""
    pass


class MetadataKeyException(RuntimeError):
    def __init__(self,
                 message: str = "",
                 keys: Optional[List[str]] = None):

        RuntimeError.__init__(self, message)
        self.message = message
        self.keys = keys or []

    def to_str(self):
        return (
            "MetadataKeyException("
            + ensure_unicode(self.message)
            + ": "
            + ", ".join(map(ensure_unicode, self.keys))
            + ")")

    def __str__(self):
        return self.to_str()


class NoMetadataStoreFound(InsufficientArgumentsError):
    pass


class ExtractorNotFoundError(InsufficientArgumentsError):
    pass
