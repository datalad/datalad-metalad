""" Metadata indexer base class """

from typing import Dict, List, Union


class MetadataIndexer(object):
    """ Defines the indexer protocol """

    def __call__(self, metadata_format_name: str, metadata: Union[Dict, List] ) -> Dict:
        """
        Create a metadata index from metadata.

        The input is a list or dictionary that contains metadata
        in the format identified by metadata_format_name.

        The output should be a set of key-value pairs that represent
        the information stored in `metadata´.

        Parameters
        ----------
        metadata_format_name: str
          The name of the metadata object, as emitted by the extractor

        metadata : Dict or List
          Metadata created by an extractor.

        Returns
        ----------
        Dictionary:
           key-value pairs representing the information in metadata.
           values can be literals or lists of literals
        """
        raise NotImplementedError
