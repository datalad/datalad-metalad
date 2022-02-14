import enum
from typing import Optional
from uuid import UUID


from dataladmetadatamodel.metadatapath import MetadataPath


class MetadataURLScheme(enum.Enum):
    UUID = "uuid"
    TREE = "tree"


class MetadataURL:
    def __init__(self,
                 local_path: Optional[MetadataPath],
                 version: Optional[str] = None):

        self.local_path = local_path
        self.version = version


class TreeMetadataURL(MetadataURL):
    def __init__(self,
                 dataset_path: MetadataPath,
                 local_path: Optional[MetadataPath],
                 version: Optional[str] = None):

        super().__init__(local_path, version)
        self.dataset_path = dataset_path


class UUIDMetadataURL(MetadataURL):
    def __init__(self,
                 uuid: UUID,
                 local_path: Optional[MetadataPath],
                 version: Optional[str] = None):

        super().__init__(local_path, version)
        self.uuid = uuid


class MetadataURLParser(object):
    uuid_header = MetadataURLScheme.UUID.value + ":"
    tree_header = MetadataURLScheme.TREE.value + ":"

    uuid_string_length = 36

    def __init__(self, path_spec: str):
        self.path_spec = path_spec
        self.current_spec = self.path_spec[:]

    def match(self, content: str):
        if self.current_spec.startswith(content):
            self.current_spec = self.current_spec[len(content):]
            return True
        return False

    def fetch_upto(self, pattern: str):
        pattern_location = self.current_spec.find(pattern)
        if pattern_location >= 0:
            result, self.current_spec = (
                self.current_spec[:pattern_location],
                self.current_spec[pattern_location:])
            return True, result
        return False, ""

    def fetch(self, length: int):
        result, self.current_spec = (
            self.current_spec[:length],
            self.current_spec[length:])
        return result

    def get_remaining(self):
        result, self.current_spec = self.current_spec, ""
        return result

    def get_path(self):
        if self.match(":"):
            path = MetadataPath(self.get_remaining())
            return True, path
        return False, MetadataPath("")

    def parse_version(self):
        if self.match("@"):
            # a version string ends either with a ":" or
            # with the end of the token stream
            success, version = self.fetch_upto(":")
            if success:
                return True, version
            return True, self.get_remaining()
        return False, None

    def parse(self) -> MetadataURL:
        """
        Parse a metadata path spec. It can either be a uuid spec or a tree
        spec. If no scheme is provided, a tree-spec is assumed. Note, if a
        dataset_path is empty, the root dataset is assumed and the primary
        data version of the youngest metadata record will be chosen.

        UUID:   "uuid:" UUID-DIGITS ["@" VERSION-DIGITS] [":" [LOCAL_PATH]]
        TREE:   ["tree:"] [DATASET_PATH] ["@" VERSION-DIGITS] [":" [LOCAL_PATH]]
        """

        # Try to parse a uuid-spec
        if self.match(MetadataURLParser.uuid_header):
            uuid = UUID(self.fetch(MetadataURLParser.uuid_string_length))
            _, version = self.parse_version()
            _, local_path = self.get_path()
            return UUIDMetadataURL(uuid, local_path, version)

        # Expect a tree spec
        self.match(self.tree_header)

        success, dataset_path = self.fetch_upto("@")
        if success:
            dataset_path = MetadataPath(dataset_path)
            _, version = self.parse_version()
            self.match(":")
            local_path = MetadataPath(self.get_remaining())
        else:
            version = None
            success, dataset_path = self.fetch_upto(":")
            if success:
                dataset_path = MetadataPath(dataset_path)
                _, local_path = self.get_path()
            else:
                dataset_path = MetadataPath(self.get_remaining())
                local_path = MetadataPath("")
        return TreeMetadataURL(dataset_path, local_path, version)


def parse_metadata_url(metadata_url: str) -> MetadataURL:
    parser = MetadataURLParser(metadata_url)
    return parser.parse()
