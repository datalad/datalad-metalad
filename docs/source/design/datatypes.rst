.. -*- mode: rst -*-
.. vi: set ft=rst sts=4 ts=4 sw=4 et tw=79:

.. _chap_datatypes:


*********
Datatypes
*********

.. topic:: Specification scope and status

   This specification provides an overview over data types that are represented by classes in metalad.

Motivation for data types
-------------------------

Datalad, and historically metalad as well, use dictionaries to pass results internally and to report results externally (by converting the dictionaries into JSON-strings). Datalad code historically uses strings as dictionary keys, not constants. This approach has a few drawbacks:

1. Without proper documentation the expected and optional values are difficult to determine.

2. The direct access to elements through *string* is error prone.

3. It is not easy to determine which arguments a method accepts, or which keys a dictionary should have without additional documentation.


Action
------

Metalad therefore started to define a number of classes that represent the internally used objects in a more rigid way. Metalad uses custom classes or tools like ``dataclasses``, ``attr``, or ``pydantic``.

This metadata-type classes are defined in ``datalad_metalad.metadatatypes``. They are used for internal objects as well as for objects that are emitted as result of datalad operations like ``meta-filter``. Currently the following  metadata-type classes are defined:

- ``Result``: a baseclass for all results, that are yielded in datalad API calls

- ``MetadataResult``: a subclass of ``Result``, that represents the result of a datalad API call that yields metadata

- ``Metadata``: a dataclass, which represents a metadata record in a metadata result

