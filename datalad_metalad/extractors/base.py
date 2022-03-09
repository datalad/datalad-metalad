# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""MetadataRecord extractor base class"""
import abc
import dataclasses
import enum
from typing import (
    Any,
    IO,
    Dict,
    List,
    Optional,
    Union,
)
from uuid import UUID

from datalad.distribution.dataset import Dataset


@dataclasses.dataclass
class FileInfo:
    type: str           # TODO: state constants
    git_sha_sum: str
    byte_size: int
    state: str          # TODO: state constants
    path: str
    intra_dataset_path: str


@dataclasses.dataclass
class ExtractorResult:
    extractor_version: str
    extraction_parameter: Dict[str, Any]
    extraction_success: bool
    datalad_result_dict: Dict[str, Any]
    immediate_data: Optional[Dict[str, Any]] = None


class DataOutputCategory(enum.Enum):
    """
    Describe how extractors output metadata.
    MetadataRecord can be small, like a few numbers,
    or large e.g. images or sets of images.

    An extractor can either output to a single file
    (FILE), or it can output a complex result,
    containing  multiple files and sub-directories
    to a directory (DIRECTORY), or it can return
    the result as immediate data in the extractor
    result object (IMMEDIATE).
    """
    FILE = 1
    DIRECTORY = 2
    IMMEDIATE = 3


class MetadataExtractorBase(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def extract(self,
                output_location: Optional[Union[IO, str]] = None
                ) -> ExtractorResult:
        """
        Run metadata extraction.

        The value of output_location depends on the data output
        category for this extractor.

        DataOutputCategory.IMMEDIATE:
        The value of output_location must be None.

        DataOutputCategory.FILE:
        The value of output_location is file descriptor for an
        empty binary file opened in read/write mode. The extractor
        should write all the metadata it outputs to the file.
        The content of the file will be added to the metadata.

        DataOutputCategory.DIRECTORY:
        The value of output_location is the path of a directory.
        The extractor should write all its output to files or
        subdirectories in the directory.
        The content of the directory will be added to the
        metadata.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_id(self) -> UUID:
        """ Report the universally unique ID of the extractor """
        raise NotImplementedError

    @abc.abstractmethod
    def get_version(self) -> str:       # TODO shall we remove this and regard it as part of the state?
        """ Report the version of the extractor """
        raise NotImplementedError

    @abc.abstractmethod
    def get_data_output_category(self) -> DataOutputCategory:
        raise NotImplementedError

    def get_state(self, dataset):
        """Report on extractor-related state and configuration

        Extractors can reimplement this method to report arbitrary information
        in a dictionary. This information will be included in the metadata
        aggregate catalog in each dataset. Consequently, this information
        should be brief/compact and limited to essential facts on a
        comprehensive state of an extractor that "fully" determines its
        behavior. Only plain key-value items, with simple values, such a string
        int, float, or lists thereof, are supported.

        State information can be dataset-specific. The respective Dataset
        object instance is passed via the method's `dataset` argument.

        The state information will be recorded together with the parameters
        that the extractor used and associated with the emitted metadata.

        Primarily, this is useful for reporting
        per-extractor version information (such as a version for the extractor
        output format, or critical version information on external software
        components employed by the extractor), and potential configuration
        settings that determine the behavior of on extractor.

        """
        return {}


class DatasetMetadataExtractor(MetadataExtractorBase, metaclass=abc.ABCMeta):
    def __init__(self,
                 dataset: Dataset,
                 ref_commit: str,
                 parameter: Optional[Dict[str, Any]] = None):
        """
        Parameters
        ----------
        dataset : Dataset
          Dataset instance to extract metadata from.

        ref_commit : str
          SHA of the commit for which metadata should be created.
          Can be used for identification purposed, such as '@id'
          properties for JSON-LD documents on the dataset.
          # TODO: can this be git tree-nodes hashes as well?

        parameter: Dict[str, Any]
          Runtime parameter for the extractor. These may or may not
          override any defaults given in the dataset configuration.
          The extractor has to report the final applied parameter
          set in get_state.
        """
        self.dataset = dataset
        self.ref_commit = ref_commit
        self.parameter = parameter or {}

    def get_required_content(self) -> bool:
        """
        Let the extractor get the content that it needs locally.
        The default implementation is to do nothing.

        Returns
        -------
        True if all required content could be fetched, False
        otherwise. If False is returned, the extractor
        infrastructure will signal an error and the extractor's
        extract method will not be called.
        """
        return True


class FileMetadataExtractor(MetadataExtractorBase, metaclass=abc.ABCMeta):
    def __init__(self,
                 dataset: Dataset,
                 ref_commit: str,
                 file_info: FileInfo,
                 parameter: Optional[Dict[str, Any]] = None):
        """
        Parameters
        ----------
        dataset : Dataset
          Dataset instance to extract metadata from.

        ref_commit : str
          SHA of the commit for which metadata should be created.
          Can be used for identification purposed, such as '@id'
          properties for JSON-LD documents on the dataset.
          # TODO: can this be git tree-nodes hashes as well?

        file_info : FileInfo
          Information about the file for which metadata should be
          generated.
          (File infos are filtered to not contain any untracked
          content, or any files that are to be ignored for the
          purpose of metadata extraction, e.g. content under
          ".dataset/metadata".)

        parameter: Dict[str, Any]
          Runtime parameter for the extractor. These may or may not
          override any defaults given in the dataset configuration.
          The extractor has to report the final applied parameter
          set in get_state.
        """
        self.dataset = dataset
        self.ref_commit = ref_commit
        self.file_info = file_info
        self.parameter = parameter

    def is_content_required(self) -> bool:
        """
        Specify whether the content of the file defined in file_info
        must be available locally. If this method returns True, the
        metadata infrastructure will attempt to make the content
        available locally before calling the extractor-method.

        The default implementation returns False, i.e. indicates
        that the content in not required locally for the extractor
        to work.

        Returns
        -------
        True if the content must be available locally, False otherwise
        """
        return False


# NB: This is the legacy interface. We keep it around to
# use existing extractors with the file-dataset dichotomy.
# We call them with either with:
#
#   a) process_type: "file" and a status object with a
#      single file
#
#   b) process_type: "dataset" and a status object with dataset
#
#  Keep around for a bit more and then remove.
#
class MetadataExtractor(metaclass=abc.ABCMeta):
    # ATM this doesn't do anything, but inheritance from this class enables
    # detection of new-style extractor API

    @abc.abstractmethod
    def __call__(self,
                 dataset: Dataset,
                 refcommit: str,
                 process_type: str,
                 status: List):
        """Run metadata extraction

        Any implementation gets a comprehensive description of a dataset
        via the `status` argument. In many scenarios this can prevent
        needless interaction with the dataset on disk, or specific
        further queries via dataset or repository methods.

        Parameters
        ----------
        dataset : Dataset
          Dataset instance to extract metadata from.
        refcommit : str
          SHA of the commit that was determined to be the last metadata-relevant
          change in the dataset. Can be used for identification purposed, such
          '@id' properties for JSON-LD documents on the dataset.
        process_type : {'all', 'dataset', 'content'}
          Type of metadata to extract.
        status : list
          Status records produced by the `status` command for the given
          dataset. Records are filtered to not contain any untracked
          content, or any files that are to be ignored for the purpose
          of metadata extraction (e.g. content under .dataset/metadata).
          There are only records on content within the given dataset, not
          about content of any existing subdatasets.
        """
        raise NotImplementedError

    def get_required_content(self, dataset, process_type, status):
        """Report records for dataset content that must be available locally

        Any implementation can yield records in the given `status` that
        correspond to dataset content that must be available locally for an
        extractor to perform its work. It is acceptable to not yield such a
        record, or no records at all. In such case, the extractor is expected
        to handle the case of non-available content in some sensible way
        internally.

        The parameters are identical to those of
        `MetadataExtractor.__call__()`.

        Any content corresponding to a yielded record will be obtained
        automatically before metadata extraction is initiated. Hence any
        extractor reporting accurately can expect all relevant content
        to be present locally.

        Instead of a status record, it is also possible to return custom
        dictionaries that must contain a 'path' key, containing the absolute
        path to the required file within the given dataset.

        Example implementation::

            for s in status:
                if s['path'].endswith('.pdf'):
                    yield s
        """
        # be default an individual extractor is expected to manage
        # availability on its own
        return []

    def get_state(self, dataset):
        """Report on extractor-related state and configuration

        Extractors can reimplement this method to report arbitrary information
        in a dictionary. This information will be included in the metadata
        aggregate catalog in each dataset. Consequently, this information
        should be brief/compact and limited to essential facts on a
        comprehensive state of an extractor that "fully" determines its
        behavior. Only plain key-value items, with simple values, such a string
        int, float, or lists thereof, are supported.

        Any change in the reported state in comparison to a recorded state for
        an existing metadata aggregate will cause a re-extraction of metadata.
        The nature of the state change does not matter, as the entire
        dictionary will be compared.  Primarily, this is useful for reporting
        per-extractor version information (such as a version for the extractor
        output format, or critical version information on external software
        components employed by the extractor), and potential configuration
        settings that determine the behavior of on extractor.

        State information can be dataset-specific. The respective Dataset
        object instance is passed via the method's `dataset` argument.
        """
        return {}


# XXX this is the legacy-legacy interface, keep around for a bit more and then
# remove
class BaseMetadataExtractor(metaclass=abc.ABCMeta):

    NEEDS_CONTENT = True   # majority of the extractors need data content

    def __init__(self, ds, paths):
        """
        Parameters
        ----------
        ds : dataset instance
          Dataset to extract metadata from.
        paths : list
          Paths to investigate when extracting content metadata
        """

        self.ds = ds
        self.paths = paths

    def get_metadata(self, dataset=True, content=True):
        """
        Returns
        -------
        dict or None, dict or None
          Dataset metadata dict, dictionary of filepath regexes with metadata,
          dicts, each return value could be None if there is no such metadata
        """
        # default implementation
        return \
            self._get_dataset_metadata() if dataset else None, \
            ((k, v) for k, v in self._get_content_metadata()) if content else None

    @abc.abstractmethod
    def _get_dataset_metadata(self):
        """
        Returns
        -------
        dict
          keys and values are arbitrary
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _get_content_metadata(self):
        """Get ALL metadata for all dataset content.

        Possibly limited to the paths given to the extractor.

        Returns
        -------
        generator((location, metadata_dict))
        """
        raise NotImplementedError
