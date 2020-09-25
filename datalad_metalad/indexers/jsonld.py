import os
from typing import Any, Dict, List, Union

from .base import MetadataIndexer


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
        Transform a complete JSDON-LD object to an index
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

        # We know that dict_or_list_or_value is a dict now.
        if '@list' in json_ld_object:
            # Handle the @list node of JSON-LD here
            new_key_name = json_ld_object.get('@id', 'list')
            for index, element in enumerate(json_ld_object['@list']):
                yield from self._create_json_ld_index(
                    (basekey + '.' if basekey else '')
                    + '{new_key_name}[{index}]'.format(
                        new_key_name=new_key_name,
                        index=index),
                    element)

        if '@graph' in json_ld_object:
            # Handle the @graph node of JSON-LD here
            for index, element in enumerate(json_ld_object['@graph']):
                yield from self._create_json_ld_index(
                    (basekey + '.' if basekey else '')
                    + 'graph[{index}]'.format(index=index),
                    element)

        if '@type' in json_ld_object:
            type_key = self._encode_key(json_ld_object['@type'])
            basekey = u'{}{}'.format(basekey + '.' if basekey else '', type_key)

        for k, v in json_ld_object.items():
            if k in ('@type', '@list', '@graph', '@context'):
                continue
            key = self._encode_key(k)
            new_basekey = u'{}{}'.format(basekey + '.' if basekey else '', key)
            yield from self._create_json_ld_index(new_basekey, v)
        return

    def create_index(self, json_ld_object: Union[Dict, List]) -> Dict[str, Any]:
        yield from self._create_json_ld_index('', json_ld_object)
