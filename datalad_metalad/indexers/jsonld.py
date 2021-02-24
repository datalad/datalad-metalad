import enum
import os
from typing import Any, Dict, List, Union

from datalad.metadata.indexers.base import MetadataIndexer


class JsonLdTags(str, enum.Enum):
    ID = '@id'
    TYPE = '@type'
    LIST = '@list'
    GRAPH = '@graph'
    CONTEXT = '@context'


class JsonLdProperties(str, enum.Enum):
    ACCOUNTABLE_PERSON = 'accountablePerson'
    AUTHOR = 'author'
    CONTRIBUTOR = 'contributor'
    DATE_PUBLISHED = 'datePublished'
    DESCRIPTION = 'description'
    EMAIL = 'email'
    FUNDER = 'funder'
    HAS_PART = 'hasPart'
    HAS_DEFINED_TERM = 'hasDefinedTerm'
    HEADLINE = 'headline'
    KEYWORDS = 'keywords'
    NAME = 'name'
    TERM_CODE = 'termCode'
    URL = 'url'


class JsonLdTypes(str, enum.Enum):
    DATASET = 'Dataset'
    DEFINED_TERM = 'DefinedTerm'
    DEFINED_TERM_SET = 'DefinedTermSet'


class JsonLdIndexer(MetadataIndexer):
    """
    A generic JSON-LD indexer.

    Inherit from it, or use it as is for your metadata format (by adding this
    indexer as entry point for your metadata format name in setup.py)
    """

    @staticmethod
    def _encode_key(key: str) -> str:
        return (key.lstrip('@')
                .replace(os.sep, '_')
                .replace(' ', '_')
                .replace('-', '_')
                .replace('.', '_')
                .replace(':', '-'))

    def _create_json_ld_index(self, basekey, json_ld_object):
        """
        Transform a complete JSON-LD object to an index, i.e.
        a set of key-value-pairs.
        """
        if json_ld_object is None:
            yield basekey, None
            return

        if not isinstance(json_ld_object, (dict, list)):
            yield basekey, str(json_ld_object)
            return

        if isinstance(json_ld_object, list):
            for index, element in enumerate(json_ld_object):
                yield from self._create_json_ld_index(
                    basekey + '[{index}]'.format(index=index),
                    element)
            return

        # We know now that json_ld_object is a dict.
        assert isinstance(json_ld_object, dict)

        if JsonLdTags.LIST in json_ld_object:

            # Handle the @list-node of JSON-LD here
            new_key_name = json_ld_object.get(JsonLdTags.ID, 'list')

            for index, element in enumerate(json_ld_object[JsonLdTags.LIST]):

                yield from self._create_json_ld_index(
                    (basekey + '.' if basekey else '')
                    + '{new_key_name}[{index}]'.format(
                        new_key_name=new_key_name,
                        index=index),
                    element)

        if JsonLdTags.GRAPH in json_ld_object:

            # Handle the @graph-node of JSON-LD here
            for index, element in enumerate(json_ld_object[JsonLdTags.GRAPH]):
                yield from self._create_json_ld_index(
                    (basekey + '.' if basekey else '')
                    + 'graph[{index}]'.format(index=index),
                    element)

        if JsonLdTags.TYPE in json_ld_object:
            type_key = self._encode_key(json_ld_object[JsonLdTags.TYPE])
            basekey = u'{}{}'.format(basekey + '.' if basekey else '', type_key)

        for k, v in json_ld_object.items():

            if k in (JsonLdTags.TYPE,
                     JsonLdTags.LIST,
                     JsonLdTags.GRAPH,
                     JsonLdTags.CONTEXT):
                continue

            key = self._encode_key(k)
            new_basekey = u'{}{}'.format(basekey + '.' if basekey else '', key)
            yield from self._create_json_ld_index(new_basekey, v)

        return

    def create_index(self,json_ld_object: Union[Dict, List]) -> Dict[str, Any]:
        yield from self._create_json_ld_index('', json_ld_object)
