.. _chap_modref:

*******************
Metadata extractors
*******************

To use any of the contained extractors their names needs to be prefixed with
`metalad_`, such that the `runprov` extractor is effectively named
`metalad_runprov`.

Next generation extractors
--------------------------
.. currentmodule:: datalad_metalad.extractors
.. autosummary::
   :toctree: generated

   core
   annex
   custom
   runprov
   studyminimeta

Legacy extractors
-----------------

A number of legacy extractors are kept in the source code of this repository.

.. currentmodule:: datalad_metalad.extractors.legacy
.. autosummary::
   :toctree: generated

   annex
   datacite
   datalad_core
   datalad_rfc822
   frictionless_datapackage
   image
