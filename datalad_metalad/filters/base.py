# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Metadata indexer base class"""
import abc
from typing import Dict, Optional
from uuid import UUID


# TODO: parts of this definition are identical to extractors, shall we
#  base them on the same ancestor class?


class MetadataFilterBase(metaclass=abc.ABCMeta):
    def filter(self,
               metadata: Dict
               ) -> Optional[Dict]:
        """
        Run a metadata filter.

        Parameters
        ----------
        metadata: the metadata to filter

        Returns
        -------
        filtered metadata or None
        """
        raise NotImplementedError

    def get_id(self) -> UUID:
        """ Report the universally unique ID of the filter """
        raise NotImplementedError

    def get_version(self) -> str:       # TODO shall we remove this and regard it as part of the state?
        """ Report the version of the filter """
        raise NotImplementedError
