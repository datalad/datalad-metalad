DataLad extension for semantic metadata handling
************************************************

This software is a `DataLad <http://datalad.org>`__ extension that equips DataLad
with an alternative command suite for metadata handling (extraction, aggregation,
reporting). It is backward-compatible with the metadata storage format in DataLad
proper, while being substantially more performant (especially on large dataset
hierarchies). Additionally, it provides new metadata extractors and improved
variants of DataLad's own ones that are tuned for better performance and richer,
JSON-LD compliant metadata reports.


API
===

High-level API commands
-----------------------

These commands provide and improved and extended equivalent to the `metadata`
and `aggregate_metadata` commands (and the primitive `extract-metadata` plugin)
that ship with the DataLad core package.

.. currentmodule:: datalad.api
.. autosummary::
   :toctree: generated

   meta_extract
   meta_aggregate
   meta_dump

Metadata extractors
-------------------

To use any of the contained extractors their names needs to be prefixed with
`metalad_`, such that the `runprov` extractor is effectively named
`metalad_runprov`.

.. currentmodule:: datalad_metalad.extractors
.. autosummary::
   :toctree: generated

   core
   annex
   custom
   runprov

Acknowledgments
===============

DataLad development is being performed as part of a US-German collaboration in
computational neuroscience (CRCNS) project "DataGit: converging catalogues,
warehouses, and deployment logistics into a federated 'data distribution'"
(Halchenko_/Hanke_), co-funded by the US National Science Foundation (`NSF
1429999`_) and the German Federal Ministry of Education and Research (`BMBF
01GQ1411`_). Additional support is provided by the German federal state of
Saxony-Anhalt and the European Regional Development
Fund (ERDF), Project: `Center for Behavioral Brain Sciences`_, Imaging Platform

DataLad is built atop the git-annex_ software that is being developed and
maintained by `Joey Hess`_.

.. _Halchenko: http://haxbylab.dartmouth.edu/ppl/yarik.html
.. _Hanke: http://www.psychoinformatics.de
.. _NSF 1429999: http://www.nsf.gov/awardsearch/showAward?AWD_ID=1429999
.. _BMBF 01GQ1411: http://www.gesundheitsforschung-bmbf.de/de/2550.php
.. _Center for Behavioral Brain Sciences: http://cbbs.eu/en/
.. _git-annex: http://git-annex.branchable.com
.. _Joey Hess: https://joeyh.name

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. |---| unicode:: U+02014 .. em dash

.. _Git: https://git-scm.com
