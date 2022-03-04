# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""MetadataRecord indexer base class"""
import abc
from typing import (
    Iterable,
    List,
    Optional,
)
from uuid import UUID

from ..metadatatypes.metadata import MetadataRecord


__docformat__ = "restructuredtext"


# TODO: parts of this definition are identical to extractors, shall we
#  base them on the same ancestor class?

class MetadataFilterBase(metaclass=abc.ABCMeta):
    def __init__(self, format_name: Optional[str] = None):
        pass

    @abc.abstractmethod
    def filter(self,
               metadata_iterables: List[Iterable[MetadataRecord]],
               *args,
               **kwargs
               ) -> Iterable[MetadataRecord]:
        """ Entry for the filter operation

        This method is called by the 'meta-filter' driver. It should iterate
        through the metadata instances that are provided by the metadata
        coordinates, perform the filter operation ond yield the resulting
        metadata objects as instances of "datalad_metalad.metadatatypes.Metadata".

        Returned metadata is emitted as datalad invocation result, e.g. as
        JSON records.

        :param metadata_iterables:
               A list of iterables that correspond to the metadata urls that
               were given to the "meta-filter" command. Each iterable will yield
               metadata that is matched by the respective URL. Metadata is
               represented by the class "datalad_metalad.metadatatypes.Metadata".
        :param kwargs: keyword arguments that were provided in the meta-filter
               call
        :return: an iterable that contains the filtered/generated metadata as
               instances of "datalad_metalad.metadatatypes.Metadata".
        :rtype: Iterable[MetadataRecord]
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_id(self) -> UUID:
        """ Report the universally unique ID of the filter """
        raise NotImplementedError

    # TODO shall we remove 'get_version()' and regard it as part of the state?
    @abc.abstractmethod
    def get_version(self) -> str:
        """ Report the version of the filter """
        raise NotImplementedError
