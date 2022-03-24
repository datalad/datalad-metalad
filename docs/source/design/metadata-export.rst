.. -*- mode: rst -*-
.. vi: set ft=rst sts=4 ts=4 sw=4 et tw=79:

.. _chap_design_export:

**********************
Meta export components
**********************

.. topic:: Specification scope and status

   This document describes the file-system layout that
   is used by the command :command:`meta-export`.

The purpose of the command :command:`meta-export` is to generate
a file-based representation of the metadata stored in a metadata-store.
The file-based representation is also used by the command
:command:`meta-import`.

The following hierarchical structures are represented in the
exported data.

1. Dataset ID (UUID) (with 3-level de-clogging)

2. Dataset Version (commit-hash) (with 2-level de-clogging)

   2.1.  Subdataset-Tree info (JSON-file with path => (UUID, version) or null)

   2.2.  Dataset-level metadata object id, which is stored in "dataset-level-metadata.id" (kind of a link into the object-store).

   2.3.  File-level metadata info (JSON-file with file path => file-metadata-object-id)
         Metadata-objects:
            JSON object stored under file-metadata-object-id (2-level de-clogging)


Example::

       version.json
       01/
         23/
           4567-89ab-cdef-23456789abcd/
             ab/
               0123234987345897234234aa09834234/
                  dataset-level-metadata.id
                  file-tree.json
                  objects/
                    ab/
                      cdef110002349283409123123.json
                    01/
                      7890890834234aaaaa3423423.json




Discontinued and disregarded alternative file tree representation

   2.2.  File-Tree-based export:
         <recreated file-tree in which original files are directories? Problem: name-clashes, because you have
          to somehow identify the file-dir.>

        Example::

           version.json
           root-dataset-id.json
           unknown
              |
              <leading-file-tree to know subdataset>

           <root-dataset-version>
              d_<sub-dir-1>
                 f_<file>/metadata-format/parameter/metadata-content
                 d_<sub-dir-2>
                   D_<subdataset-dir>
                   S_<subdataset-dir>-dataset-level/metadata-format/parameter/metadata-content
              |
