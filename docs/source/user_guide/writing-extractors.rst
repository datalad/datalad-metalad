.. -*- mode: rst -*-
.. vi: set ft=rst sts=4 ts=4 sw=4 et tw=79:

.. _chap_writing-extractors:

Writing custom extractors
*************************

An extractor, in MetaLad sense, is a class derived from one of the base extractor classes defined in ``datalad_metalad.extractors.base`` (``DatasetMetadataExtractor`` or ``FileMetadataExtractor``).
It needs to implement several required methods, most notably ``extract()``.

Example extractors can be found in MetaLad source code.

.. TODO:: Add links to the example extractors

Base class
==========

There are two primary types of extractors, dataset-level and file-level.

Dataset-level extractors, by inheritance from the ``DatasetMetadataExtractor`` class, can access the dataset on which they operate as ``self.dataset``. Extractor functions may use it to call any dataset methods.

File-level extractors, by inheritance from the ``FileMetadataExtractor``, can refer to the file on which it operates through ``self.file_info``.

Required methods
================

``get_id()``
------------

This function should return a ``UUID`` object containing a UUID which is not used by another metalad-extractor or metalad-filter.
Since it is meant to uniquely identify the extractor, its return value should be hardcoded, or generated in a deterministic way.

A UUID can be generated in several ways, e.g. with Python (``import uuid; print(uuid.uuid4())``), or online generators
(e.g. https://www.uuidgenerator.net/ or by searching 'UUID' in `DuckDuckGo <https://duckduckgo.com/>`_).

While version 4 (random) UUIDs are probably sufficient, version 5 UUIDs (generated based on a given namespace and name) can also be used.
For example, extractors in packages developed under the datalad organisation could use ``<extractor_name>.datalad.org``.
Example generation with Python: ``uuid.uuid5(uuid.NAMESPACE_DNS, "bids_dataset.datalad.org")``.

Example::

  def get_id(self) -> UUID:
      return UUID("d1e3f728-7868-4f33-a4a5-0f9f57dce342")

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

This function should return a ``DataOutputCategory`` object, which tells MetaLad what kind of (meta)data it is dealing with.

.. TODO:: What categories are available, what are the consequences of declaring one?

Example::
  
  def get_data_output_category(self) -> DataOutputCategory:
      return DataOutputCategory.IMMEDIATE

``get_required_content()``
--------------------------

This function is used in dataset-extractors only.
It will be called by MetaLad prior to metadata extraction.
Its purpose is to make sure that content required for metadata extraction is present
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
It is a file-extractor counterpart to ``get_required_content``.
Its purpose is to tell MetaLad if the file content is required or not
(relevant for annexed files - extraction may depend on file content, or require only annex key).
If the function returns ``True``, MetaLad will get the file content.
If it returns ``False``, the get operation will not be performed.
      
``extract()``
-------------

This function is used for actual metadata extraction, and should return an ``ExtractorResult`` object.
The ``ExtractorResult`` is a `dataclass <https://docs.python.org/3/library/dataclasses.html>`_ object, containing the following fields: ``extractor_version``, ``extraction_parameter``, ``extraction_success``, ``datalad_result_dict``, and ``immediate_data`` (optional).

.. TODO:: Explain these fields, especially ``datalad_result_dict``

The ``immediate_data`` field should be a dictionary with freely-chosen keys.
Contents of this dictionary should be JSON-serializable, because ``datalad meta-extract`` will print the JSON-serialized extractor result to standard output.

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
It is recommended to give the extractor names a prefix, to reduce the risk of name collisions.

Example::
  
  [options.entry_points]
  # (...)
  datalad.metadata.extractors =
    hello_cff = datalad_helloworld.extractors.basic_dataset:CffExtractor

.. TODO::
   - How to pass options to extractors?
   - A tip on using ls-tree to discover contents
