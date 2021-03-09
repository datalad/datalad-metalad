import enum
from typing import Optional
from uuid import UUID


class MetadataPathScheme(enum.Enum):
    UUID = "uuid"
    TREE = "tree"


class MetadataPath:
    def __init__(self, local_path: Optional[str], version: Optional[str] = None):
        self.local_path = local_path
        self.version = version


class TreeMetadataPath(MetadataPath):
    def __init__(self, dataset_path: str, local_path: Optional[str], version: Optional[str] = None):
        super().__init__(local_path, version)
        self.dataset_path = dataset_path


class UUIDMetadataPath(MetadataPath):
    def __init__(self, uuid: UUID, local_path: Optional[str], version: Optional[str] = None):
        super().__init__(local_path, version)
        self.uuid = uuid


class MetadataPathParser(object):
    uuid_header = MetadataPathScheme.UUID.value + ":"
    tree_header = MetadataPathScheme.TREE.value + ":"

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
            result, self.current_spec = self.current_spec[:pattern_location], self.current_spec[pattern_location:]
            return True, result
        return False, ""

    def fetch(self, length: int):
        result, self.current_spec = self.current_spec[:length], self.current_spec[length:]
        return result

    def get_remaining(self):
        result, self.current_spec = self.current_spec, ""
        return result

    def get_path(self):
        if self.match(":"):
            path = self.get_remaining()
            return True, path
        return False, ""

    def parse_version(self):
        if self.match("@"):
            # a version string ends either with a ":" or with the end of the token stream
            success, version = self.fetch_upto(":")
            if success:
                return True, version
            return True, self.get_remaining()
        return False, None

    def parse(self):
        """
        Parse a metadata path spec. It can either be a uuid spec or a tree
        spec. If no scheme is provided, a tree-spec is assumed. Note, if a
        dataset_path is empty, the root dataset is assumed and the primary
        data version of the youngest metadata record will be chosen.

        UUID:   "uuid:" UUID-DIGITS ["@" VERSION-DIGITS] [":" [LOCAL_PATH]]
        TREE:   ["tree:"] [DATASET_PATH] ["@" VERSION-DIGITS] [":" [LOCAL_PATH]]
        """

        # Try to parse a uuid-spec
        if self.match(MetadataPathParser.uuid_header):
            uuid = UUID(self.fetch(MetadataPathParser.uuid_string_length))
            _, version = self.parse_version()
            _, local_path = self.get_path()
            return UUIDMetadataPath(uuid, local_path, version)

        # Expect a tree spec
        self.match(self.tree_header)

        success, dataset_path = self.fetch_upto("@")
        if success:
            _, version = self.parse_version()
            self.match(":")
            local_path = self.get_remaining()
        else:
            version = None
            success, dataset_path = self.fetch_upto(":")
            if success:
                _, local_path = self.get_path()
            else:
                dataset_path = self.get_remaining()
                local_path = ""
        return TreeMetadataPath(dataset_path, local_path, version)
