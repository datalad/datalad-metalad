import os
from typing import (
    Any,
    Dict,
    List,
    Union,
    cast
)

from datalad.metadata.indexers.base import MetadataIndexer


class IndexerJsonLdTags:
    ID = '@id'
    TYPE = '@type'
    LIST = '@list'
    GRAPH = '@graph'
    CONTEXT = '@context'


class IndexerSchemaOrgProperties:
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


class IndexerSchemaOrgTypes:
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

    def _create_json_ld_index(self, base_key, json_ld_object):
        """
        Transform a complete JSON-LD object to an index, i.e.
        a set of key-value-pairs.
        """
        if json_ld_object is None:
            yield base_key, None
            return

        if not isinstance(json_ld_object, (dict, list)):
            yield base_key, str(json_ld_object)
            return

        if isinstance(json_ld_object, list):
            for index, element in enumerate(json_ld_object):
                yield from self._create_json_ld_index(
                    base_key + '[{index}]'.format(index=index),
                    element)
            return

        # We know now that json_ld_object is a dict.
        json_ld_object = cast(dict, json_ld_object)

        if IndexerJsonLdTags.LIST in json_ld_object:

            # Handle the @list-node of JSON-LD here
            new_key_name = json_ld_object.get(IndexerJsonLdTags.ID, 'list')

            for index, element in enumerate(json_ld_object[IndexerJsonLdTags.LIST]):

                yield from self._create_json_ld_index(
                    (base_key + '.' if base_key else '')
                    + '{new_key_name}[{index}]'.format(
                        new_key_name=new_key_name,
                        index=index),
                    element)

        if IndexerJsonLdTags.GRAPH in json_ld_object:

            # Handle the @graph-node of JSON-LD here
            for index, element in enumerate(json_ld_object[IndexerJsonLdTags.GRAPH]):
                yield from self._create_json_ld_index(
                    (base_key + '.' if base_key else '')
                    + 'graph[{index}]'.format(index=index),
                    element)

        if IndexerJsonLdTags.TYPE in json_ld_object:
            type_key = self._encode_key(json_ld_object[IndexerJsonLdTags.TYPE])
            base_key = u'{}{}'.format(base_key + '.' if base_key else '', type_key)

        for k, v in json_ld_object.items():

            if k in (IndexerJsonLdTags.TYPE,
                     IndexerJsonLdTags.LIST,
                     IndexerJsonLdTags.GRAPH,
                     IndexerJsonLdTags.CONTEXT):
                continue

            key = self._encode_key(k)
            new_base_key = u'{}{}'.format(base_key + '.' if base_key else '', key)
            yield from self._create_json_ld_index(new_base_key, v)

        return

    def create_index(self, json_ld_object: Union[Dict, List]) -> Dict[str, Any]:
        yield from self._create_json_ld_index('', json_ld_object)
