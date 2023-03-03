DataLad extension for semantic metadata handling
************************************************

This software is a `DataLad <http://datalad.org>`__ extension that equips DataLad
with an alternative command suite for metadata handling (extraction, aggregation,
reporting). It is backward-compatible with the metadata storage format in DataLad
proper, while being substantially more performant (especially on large dataset
hierarchies). Additionally, it provides new metadata extractors and improved
variants of DataLad's own ones that are tuned for better performance and richer,
JSON-LD compliant metadata reports.

Commands and API
================

.. toctree::
   :maxdepth: 2

   api
   extractors

User guide
----------

.. toctree::
   :maxdepth: 2

   user_guide/index

Concepts and technologies
-------------------------

.. toctree::
   :maxdepth: 2

   design/index
   acknowledgements




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. |---| unicode:: U+02014 .. em dash

.. _Git: https://git-scm.com
