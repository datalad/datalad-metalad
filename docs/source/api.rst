.. _chap_cmdline:

***********************
High-level API commands
***********************

These commands provide and improved and extended equivalent to the `metadata`
and `aggregate_metadata` commands (and the primitive `extract-metadata` plugin)
that ship with the DataLad core package.

Commandline reference
---------------------

.. toctree::
   :maxdepth: 1

   generated/man/datalad-meta-add
   generated/man/datalad-meta-extract
   generated/man/datalad-meta-aggregate
   generated/man/datalad-meta-dump
   generated/man/datalad-meta-conduct
   generated/man/datalad-meta-filter


Python module reference
-----------------------

.. currentmodule:: datalad.api
.. autosummary::
   :toctree: generated

   meta_add
   meta_extract
   meta_aggregate
   meta_dump
   meta_conduct
   meta_filter
