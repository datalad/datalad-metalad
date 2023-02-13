.. -*- mode: rst -*-
.. vi: set ft=rst sts=4 ts=4 sw=4 et tw=79:

.. _chap_writing-extractors:

Writing custom extractors
*************************
Metalad supports the automated extraction of metadata through a common single interface that allows the execution of metadata extractors.
An extractor, in MetaLad sense, is a class derived from one of the base extractor classes defined in ``datalad_metalad.extractors.base`` (``DatasetMetadataExtractor`` or ``FileMetadataExtractor``).
It needs to implement several required methods, most notably ``extract()``.

Example extractors can be found in MetaLad source code:

- `Dataset-level extractor example <https://github.com/datalad/datalad-metalad/blob/master/datalad_metalad/extractors/metalad_example_dataset.py>`_

- `File-level extractor example <https://github.com/datalad/datalad-metalad/blob/master/datalad_metalad/extractors/metalad_example_file.py>`_


Base class
==========

There are two primary types of extractors, dataset-level extractors and file-level extractors.

Dataset-level extractors, by inheritance from the ``DatasetMetadataExtractor`` class, can access the dataset on which they operate as ``self.dataset``.
Extractor functions may use this object to call any DataLad dataset methods. They can perform whatever operations they deem necessary to extract metadata from the dataset, for example, they could count the files in the dataset or look for a file named ``CITATION.cff`` in the root directory of the dataset and return its content.

File-level extractors, by inheritance from the ``FileMetadataExtractor``, contain a ``Dataset``-object in the property ``self.dataset`` and a `FileInfo`-object in the propery ``self.file_info``. ``FileInfo`` is a `dataclass <https://docs.python.org/3/library/dataclasses.html>`_ with the properties ``type``, ``git_sha_sum``, ``byte_size``, ``state``, ``path``, and ``intra_dataset_path`` fields. File-level extractors should return metadata that describes the file that is referenced by ``FileInfo``.

Required methods
================
Any extractor is expected to implement a number of abstract methods that define the interface through which MetaLad communicates with extractors.

``get_id()``
------------

This function should return a ``UUID`` object containing a UUID which is not used by another metalad-extractor or metalad-filter.
Since it is meant to uniquely identify the extractor and the type of data it returns, its return value should be hardcoded, or generated in a deterministic way.

A UUID can be generated in several ways, e.g. with Python (``import uuid; print(uuid.uuid4())``), or online generators
(e.g. https://www.uuidgenerator.net/ or by searching 'UUID' in `DuckDuckGo <https://duckduckgo.com/>`_).

While version 4 (random) UUIDs are probably sufficient, version 5 UUIDs (generated based on a given namespace and name) can also be used.
For example, extractors in packages developed under the datalad organisation could use ``<extractor_name>.datalad.org``.
Example generation with Python: ``uuid.uuid5(uuid.NAMESPACE_DNS, "bids_dataset.extractors.datalad.org")``.

Example::

  def get_id(self) -> UUID:
      return UUID("0c26f218-537e-5db8-86e5-bc12b95a679e")

``get_version()``
-----------------

This function should return a version string representing the extractor's version.

The extractor version is meant to be included with the extracted metadata.
It might be a version number, or a reference to a particular schema.
While there are no requirements on the versioning scheme, it should be chosen carefully, and rules for version updating need to be considered.
For example, when using `semantic versioning <https://semver.org/>`_ style, one might decide to guarantee that attribute type and name will never change within a major release, while new attributes may be added with a minor release.

Example::

  def get_version(self) -> str:
      return "0.0.1"

``get_data_output_category()``
------------------------------

This function should return a ``DataOutputCategory`` object, which tells MetaLad what kind of (meta)data it is dealing with. The following output categories are available in the enumeration ``datalad_metalad.extractors.base.DataOutputCategory``::

 IMMEDIATE
 FILE
 DIRECTORY

Output categories declare how the extractor delivers its results. If the output category is ``IMMEDIATE``, the result is returned by the ``extract()``-call. In case of ``FILE``, the extractor deposits the result in a file. This ie especially useful for extractors that are external programs. The category ``DIRECTORY`` is not yet supported. If you write an extractor in Python, you would usually use the output category ``IMMEDIATE`` and return extractor results from the ``extract()``-call.

Example::
  
  def get_data_output_category(self) -> DataOutputCategory:
      return DataOutputCategory.IMMEDIATE

``get_required_content()``
--------------------------

This function is used in dataset-level extractors only.
It will be called by MetaLad prior to metadata extraction.
Its purpose is to allow the extractor to ensure that content that is required for metadata extraction is present
(relevant, for example, if some of files to be inspected may be annexed).
The function should return ``True`` if it has obtained the required content, or confirmed its presence.
If it returns ``False``, metadata extraction will not proceed.

This function can be a place to call ``dataset.get()``.
It is advisable to disable result rendering (``result_renderer="disabled"``), because during metadata extraction, users will typically want to redirect standard output to a file or another command.

Example::

  def get_required_content(self) -> bool:
      result = self.dataset.get("CITATION.cff", result_renderer="disabled")
      return result[0]["status"] in ("ok", "notneeded")

``is_content_required()``
-------------------------

This function is used in file-extractors only.
It is a file-extractor counterpart to ``get_required_content()``.
Its purpose is to tell MetaLad whether the file content is required or not
(relevant for annexed files - extraction may depend on file content, or require only annex key).
If the function returns ``True``, MetaLad will get the file content.
If it returns ``False``, the get operation will not be performed.
      
``extract()``
-------------

This function is used for actual metadata extraction. It has one parameter called ``output_location``. If the output category of the extractor is ``DataOutputCategory.IMMEDIATE``, this parameter will be ``None``. If the output category is ``DataOutputCategory.FILE``. this parameter will contain either a file name or a ``file``-object into which the extractor can write its output.

 The function should return an ``datalad_metalad.extractors.base.ExtractorResult`` object.
The ``ExtractorResult`` is a `dataclass <https://docs.python.org/3/library/dataclasses.html>`_ object, containing the following fields:

- ``extractor_version``: a version string representing the extractor's version.
-  ``extraction_parameter``: a dictionary containing parameters passed to the extractor by the calling command; can be obtained with: ``self.parameter or {}``.
-  ``extraction_success``: either ``True`` or ``False``.
-  ``datalad_result_dict``: a dictionary with entries added to the DataLad `result record <https://docs.datalad.org/en/stable/design/result_records.html>`_ produced by a MetaLad calling command. Result records are used by DataLad to inform generic error handling and decisions on how to proceed with subsequent operations. MetaLad commands always set the mandatory result record fields ``action`` and ``path``; the minimally useful set of fields which should by the extractor is ``"status"`` (one of: ``"ok"``, ``"notneeded"``, ``"impossible"``, ``"error"``) and ``"type"`` (``"dataset"`` or ``"file"``).
- ``immediate_data`` (a dictionary, optional): if the output category of the extractor is ``IMMEDIATE``, then the ``immediate_data`` field should contain the result of the extraction process as a dictionary with freely-chosen keys. Contents of this dictionary should be JSON-serializable, because ``datalad meta-extract`` will print the JSON-serialized extractor result to standard output.

Example::

  def extract(self, _=None) -> ExtractorResult:
      # Returns citation file content as metadata, altering only date

      # load file, guaranteed to be present
      with open(Path(self.dataset.path) / "CITATION.cff") as f:
          yamlContent = yaml.safe_load(f)
  
      # iso-format dates (nonexhaustive - publications have them too)
      if "date-released" in yamlContent:
          isodate = yamlContent["date-released"].isoformat()
          yamlContent["date-released"] = isodate

      return ExtractorResult(
          extractor_version=self.get_version(),
	  extraction_parameter=self.parameter or {},
	  extraction_success=True,
	  datalad_result_dict={
	      "type": "dataset",
	      "status": "ok",
	  },
	  immediate_data=yamlContent,
      )

Making extractors discoverable
==============================

To be discovered by ``meta_extract``, an extractor should be part of a DataLad extension.
In addition, to make it discoverable, you need to declare an entry point in the extension's ``setup.cfg`` file.
You can define the entrypoint name, and specify which extractor class it should point to.
It is recommended to give the extractor name a prefix, to reduce the risk of name collisions.

Example::
  
  [options.entry_points]
  # (...)
  datalad.metadata.extractors =
    hello_cff = datalad_helloworld.extractors.basic_dataset:CffExtractor

Tips
====

Using git methods to discover contents efficiently
--------------------------------------------------

Dataset-level extractors may need to check specific files to obtain information about specific files.
If the files need to be listed, it may be more efficient to call `git-ls-files <https://git-scm.com/docs/git-ls-files>`_ or `git-ls-tree <https://git-scm.com/docs/git-ls-tree>`_ instead of using pathlib methods (this limits the listing to files tracked by the dataset and helps avoid costly indexing if the `.git` directory).
For example, a list of files with a given extension (including those in subfolders) can be created with::

  files = list(self.dataset.repo.call_git_items_(["ls-files", "*.xyz"]))

.. TODO::
   - Explain how extractors can use extraction parameters passed to a calling command
   - Include a short description of how ``metalad_external_[dataset|file]`` extractors can be an alternative to writing custom extractors if an external process for generating metadata already exists
