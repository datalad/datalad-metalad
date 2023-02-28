Meet Metalad, an extension to datalad that supports metadata handling
*********************************************************************

This chapter will show you how to install ``metalad``, create a datalad
dataset and how to work with metadata. Working with metadata means,
adding metadata, viewing metadata, and aggregating metadata.

Below you will find examples and instructions on how to install metalad,
how to create an example dataset and how to work with metadata. You will
learn how to add metadata, how to display metadata that is stored
locally or remotely, and how to combine (aka aggregate) metadata from
multiple datasets.

Preparations
=============

Install metalad and create a datalad dataset

Install metalad
---------------

Create a virtual python environment and activate it (Linux example shown).

.. code:: shell

   > python3 -m venv venv/datalad-metalad
   > source venv/datalad-metalad/bin/activate

Install the latest version of metalad

.. code:: shell

   > pip install --upgrade datalad-metalad

Create a datalad dataset
------------------------

The following command creates a datalad dataset that stores text-files
in git and non-text files in git-annex.

.. code:: shell

   > datalad create -c text2git example-ds

Add a text-file to the dataset

.. code:: shell

   > cd example-ds
   > echo some content > file1.txt
   > datalad save

Add a binary file to the dataset

.. code:: shell

   > dd if=/dev/zero of=file.bin count=1000
   > datalad save

Working with metadata
=====================

This chapter provides an overview of commands in metalad. If you want to
continue with the hands-on examples, skip to next chapter (and probably
come back here later).

Metalad allows you to *associate* metadata with datalad datasets or
files in datalad-datasets. More specifically, metadata can be associated
with: datasets, sub-datasets, and files in a dataset or sub-dataset.

Metalad can associate an arbitrary amount of individual metadata
*instances* with a single element (dataset or file). Each metadata
instance is identified by a *type-name* that specifies the type of the
data contained in the metadata instance. For example: ``metalad_core``,
``bids``, etc.

.. note:: Implementation side note:
   The metadata associations can in principle be stored in any
   git-repository, but are by default stored in the git-repository of
   a root dataset.

The plumbing
------------

Metalad has a few basic commands, aka plumbing commands, that perform
essential elementary metadata operations:

-  ``meta-add`` add metadata to an element (dataset, sub-dataset, or
   file)

-  ``meta-dump`` show metadata stored in a local or remote dataset


The porcelain
-------------

To simplify working with metadata, metalad provides a number of higher
level functions and tools that implement typical use cases.

-  ``meta-extract`` run an extractor (see below) on an existing dataset,
   sub-dataset, or file and emit metadata that can be fed to
   ``meta-add``.

-  ``meta-conduct`` run pipelines of extractors and adders on locally
   available datatasets, sub-datasets and files, in order to automatate
   metadata extraction and adding tasks

-  ``meta-aggregate`` combine metadata from number of sub-datasets into
   the root-dataset.

- ``meta-filter`` walk through metadata from multiple stores, apply a
  filter, and output new metadata

Metadata extractors
-------------------

Datalad supports pluggable metadata extractors. Metadata extractors can
perform arbitrary operations on the given element (dataset, sub-dataset,
or file) and return arbitrary metadata in JSON-format. Meta-extract will
associate the metadata with the metadata element.

Metalad comes with a number of extractors. Some extractors are provided
by metalad, some are inherited from datalad. The provided extractors
generate provenance records for datasets and data, or they extract
metadata from specific files or data-structures, e.g. BIDS. In principle
any processing is possible. There is also a generic extractor, which
allows to invoke external commands to generate metadata.

Metadata extraction examples
============================

Extract dataset-level metadata
------------------------------

Extract dataset-level metadata with the ``datalad`` command
``meta-extract``. It takes a number of optional arguments and one
required argument, the name of the metadata extractor that should be
used. We use ``metalad_core`` for now.

.. code:: shell

   > datalad meta-extract metalad_core

The extracted metadata will be written to stdout and will look similar
to this (times, names, and UUIDs will be different for you):

.. code:: json

   {"type": "dataset", "dataset_id": "853d9356-fc2e-459e-96bc-02414a1fef93", "dataset_version": "8d6d0e50a27b7540717360e21332b1ad0c924415", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extraction_time": 1637921555.282522, "agent_name": "Your Name", "agent_email": "you@example.com", "extracted_metadata": {"@context": {"@vocab": "http://schema.org/", "datalad": "http://dx.datalad.org/"}, "@graph": [{"@id": "59286713dacabfbce1cecf4c865fff5a", "@type": "agent", "name": "Your Name", "email": "you@example.com"}, {"@id": "8d6d0e50a27b7540717360e21332b1ad0c924415", "identifier": "853d9331-fc2e-459e-96bc-02414a1fef93", "@type": "Dataset", "version": "0-3-g8d6d0e5", "dateCreated": "2021-11-26T11:03:25+01:00", "dateModified": "2021-11-26T11:09:27+01:00", "hasContributor": {"@id": "59286713dacabfbce1cecf4c865fff5a"}}]}}

The output is a JSON-serialized object. You can use
```jq`` <https://stedolan.github.io/jq/>`__ to get a nicer formatting of
the JSON-object. For example the command:

.. code:: shell

   > datalad meta-extract metalad_core|jq .

would result in an output similar to:

.. code:: json

   {
     "type": "dataset",
     "dataset_id": "853d9356-fc2e-459e-96bc-02414a1fef93",
     "dataset_version": "ee512961b878a674c8068e54656e161d40566d9b",
     "extractor_name": "metalad_core",
     "extractor_version": "1",
     "extraction_parameter": {},
     "extraction_time": 1637923596.9511302,
     "agent_name": "Your Name",
     "agent_email": "you@example.com",
     "extracted_metadata": {
       "@context": {
         "@vocab": "http://schema.org/",
         "datalad": "http://dx.datalad.org/"
       },
       "@graph": [
         {
           "@id": "59286713dacabfbce1cecf4c865fff5a",
           "@type": "agent",
           "name": "Your Name",
           "email": "you@example.com"
         },
         {
           "@id": "ee512961b878a674c8068e54656e161d40566d9b",
           "identifier": "853d9356-fc2e-459e-96bc-02414a1fef93",
           "@type": "Dataset",
           "version": "0-4-gee51296",
           "dateCreated": "2021-11-26T11:03:25+01:00",
           "dateModified": "2021-11-26T11:13:58+01:00",
           "hasContributor": {
             "@id": "59286713dacabfbce1cecf4c865fff5a"
           }
         }
       ]
     }

Extract file-level metadata
---------------------------

The ``datalad`` command ``meta-extract`` also support the extraction of
file-level metadata. File-level metadata extraction requires a second
argument, besides the extractor-name, to ``datalad meta-extract``. The
second argument identifies the file for which metadata should be
extracted.

NB: you must specify an extractor that supports file-level extraction
if a file-name is passed to ``datalad meta-extract``, and an extractor
that supports dataset-level extraction if no file-name is passed to
``datalad meta-extract``. The extractor ``metalad_core`` supports both
metadata levels.

To extract metadata for the file ``file1.txt``, execute the following
command:

.. code:: shell

   > datalad meta-extract metalad_core file1.txt

which will lead to an output similar to:

.. code:: json

   {"type": "file", "dataset_id": "853d9331-fc2e-459e-96bc-02414a1fef93", "dataset_version": "ee512961b878a674c8068e54656e161d40566d9b", "path": "file1.txt", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extraction_time": 1637927097.2165475, "agent_name": "Your Name", "agent_email": "you@example.com", "extracted_metadata": {"@id": "datalad:SHA1-s13--2ef267e25bd6c6a300bb473e604b092b6a48523b", "contentbytesize": 13}}

Add metadata
============

You can add extracted metadata to the dataset (metadata will be stored
in a special area of the git-repository and not interfere with your data
in the dataset).

To add metadata you use the ``datalad`` command ``meta-add``. The
``meta-add`` command takes on required argument, the name of a file that
contains metadata in JSON-format. It also supports reading JSON-metadata
from stdin, if you provided ``-`` as the file name. That mean you can
pipe the output of ``meta-extract`` directly into ``meta-add`` by
specifying ``-`` as metadata file-name like this:

.. code:: shell

   > datalad meta-extract metalad_core |datalad meta-add -

``meta-add`` supports files that contain lists of JSON-records in “JSON
Lines”-format (see `jsonlines.org <https://jsonlines.org/>`_).

Let’s add the file-level metadata for ``file1.txt`` and ``file.bin`` to
the metadata of the dataset by executing the two commands:

.. code:: shell

   > datalad meta-extract metalad_core file1.txt |datalad meta-add -

and

.. code:: shell

   > datalad meta-extract metalad_core file.bin |datalad meta-add -

Display (retrieve) metadata
===========================

To view the metadata that has been stored in a dataset, you can use the
``datalad`` command ``meta-dump``. The following command will show all
metadata that is stored in the dataset. Metadata is displayed in JSON
Lines-format (aka newline-delimited JSON), which is a number of lines
where each line contains a serialized JSON object.

.. code:: shell

   datalad meta-dump -r

Its execution will generate a result similar to:

.. code:: json

   {"type": "dataset", "dataset_id": "853d9356-fc2e-459e-96bc-02414a1fef93", "dataset_version": "ee512961b878a674c8068e54656e161d40566d9b", "extraction_time": 1637924361.8114567, "agent_name": "Your Name", "agent_email": "you@example.com", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"@context": {"@vocab": "http://schema.org/", "datalad": "http://dx.datalad.org/"}, "@graph": [{"@id": "59286713dacabfbce1cecf4c865fff5a", "@type": "agent", "name": "Your Name", "email": "you@example.com"}, {"@id": "ee512961b878a674c8068e54656e161d40566d9b", "identifier": "853d9356-fc2e-459e-96bc-02414a1fef93", "@type": "Dataset", "version": "0-4-gee51296", "dateCreated": "2021-11-26T11:03:25+01:00", "dateModified": "2021-11-26T11:13:58+01:00", "hasContributor": {"@id": "59286713dacabfbce1cecf4c865fff5a"}}]}}
   {"type": "file", "path": "file1.txt", "dataset_id": "853d9356-fc2e-459e-96bc-02414a1fef93", "dataset_version": "ee512961b878a674c8068e54656e161d40566d9b", "extraction_time": 1637927239.2590044, "agent_name": "Your Name", "agent_email": "you@example.com", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"@id": "datalad:SHA1-s13--2ef267e25bd6c6a300bb473e604b092b6a48523b", "contentbytesize": 13}}
   {"type": "file", "path": "file.bin", "dataset_id": "853d9356-fc2e-459e-96bc-02414a1fef93", "dataset_version": "ee512961b878a674c8068e54656e161d40566d9b", "extraction_time": 1637927246.2115273, "agent_name": "Your Name", "agent_email": "you@example.com", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"@id": "datalad:MD5E-s512000--816df6f64deba63b029ca19d880ee10a.bin", "contentbytesize": 512000}}

Adding a lot of metadata with ``meta-conduct``
==============================================

To extract and add metadata from a large number of files or from all
files of a dataset you can use ``meta-conduct``. Meta-conduct can be
configured to execute a number of ``meta-extract`` and ``meta-add``
commands automatically in parallel. The operations that ``meta-conduct``
should perform are defined in pipeline definitions. A few pipeline
definitions are provided with metalad, and we will use the
``extract_metadata`` pipeline.

Adding dataset-level metadata
-----------------------------

Execute the following command:

.. code:: shell

   datalad meta-conduct extract_metadata traverser:`pwd` traverser:dataset extractor:dataset extractor:metalad_core

You will get an output which is similar to:

.. code:: shell

   meta_conduct(ok): <...>/gist/example-ds

What happened?

You just ran the ``extract_metadata`` pipeline and specified that you
want to traverse the current directory (:literal:`traverser:`pwd\``),
and that you want to operate on all datasets that are encountered
(``traverser:Dataset``). You also specified that, for each element
found during traversal, you would like to execute a dataset-level
extractor (``extractor:dataset``) with the name ``metalad_core``
(``extractor:metalad_core``).

The pipeline found one dataset in the current directory and added the
metadata to it. Since you have done that already before using
``meta-extract`` and ``meta-add``, you have the same number of
metadata entries in the metadata store. That means ``datalad meta-dump
-r`` will give you three results. But you might notice that the
``extraction-time`` of the dataset-level entry has changed.

Metalad comes with different pre-built pipelines. Some allow to
automatically fetch an annexed file and automatically drop said file,
after is has been processed.

Adding file-level metadata
--------------------------

You can also add file-metadata using ``meta-conduct``. Execute the
following command:

.. code:: shell

   datalad meta-conduct extract_metadata traverser:`pwd` traverser:file extractor:file extractor:metalad_core

You will get an output which is similar to:

.. code:: shell

   meta_conduct(ok): <...>/example-ds/file1.txt                                                         
   meta_conduct(ok): <...>/example-ds/file.bin
   action summary:
     meta_conduct (ok: 2)

What happened here?

The traverser found two elements that fitted your description
(``traverser:Dataset``), executed the specified extractor on them
(``extractor:metalad_core``), and added the results to the metadata
storage.

Again, you can verify this with the value of ``extraction_time`` in the
output of ``datalad meta-dump -r``.

Joining metadata from multiple datasets with ``meta-aggregate``
===============================================================

Let’s have a look at ``meta-aggregate``. The command ``meta-aggregate``
copies metadata from sub-datasets into the metadata store of the root
dataset.

Subdataset creation
-------------------

To see ``meta-aggregate`` in action we first create a sub-datasets:

.. code:: shell

   > datalad create -d . -c text2git subds1

This command will yield an output similar to:

.. code:: shell

   [INFO   ] Creating a new annex repo at <...>/example-ds/subds1 
   [INFO   ] Running procedure cfg_text2git 
   [INFO   ] == Command start (output follows) ===== 
   [INFO   ] == Command exit (modification check follows) =====
   add(ok): subds1 (file)
   add(ok): .gitmodules (file)
   save(ok): . (dataset)
   create(ok): subds1 (dataset)
   action summary:
     add (ok: 2)
     create (ok: 1)
     save (ok: 1)

Create some content and save it:

.. code:: shell

   > cd subds1
   > echo content of subds1/file_subds1.1.txt > file_subds1.1.txt
   > datalad save

Now run the file level extractor in the subdataset:

.. code:: shell

   > datalad meta-conduct extract_metadata traverser:`pwd` traverser:file extractor:file extractor:metalad_core

and the dataset-level extractor:

.. code:: shell

   > datalad meta-conduct extract_metadata traverser:`pwd` traverser:dataset extractor:dataset extractor:metalad_core

If you want you can view the added metadata in the subdataset with the
command ``datalad meta-dump -r``.

Since we modified the subdataset, we should also save the root dataset:

.. code:: shell

   > cd ..
   > datalad save

Aggregating
-----------

After all the above commands are executed, we have metadata stored in
two datasets (more precisely, in the metadata stores of the datasets
which are the git repositories). In the metadata store of ``example-ds``
we have the following information:

.. code:: none

   version: <version of dataset at metadata extraction>
     dataset-level:
       .:
         metalad_core: <metadata for example-ds>
     file-level:
       ./file1.txt:
         metalad_core: <metadata for file1.txt>
       ./file.bin:
         metalad_core: <metadata for file.bin>

And in the metadata store of ``subds1`` we have:

.. code:: none

   version: <version of dataset at metadata extraction>
     dataset-level:
       .:
         metalad_core: <metadata for subds1>
     file-level:
       ./file_subds1.1.txt: <metadata for file_subds1.1.txt> 

Now let us aggregate the subdataset metadata into the root dataset with
the command ``meta-aggregate``:

.. code:: shell

   > datalad meta-aggregate -d . subds1

And display the result:

.. code:: shell

   > datalad meta-dump -r

The output will contain five JSON records (in 5 lines), three from the
top-level datasets and two from the subdataset. It will look similar to
this:

.. code:: json

   {"type": "dataset", "dataset_id": "ceeb844a-c6e8-4b2f-bb7c-62b7ae449a9f", "dataset_version": "bcf9cfde4a599d26094a58efbe4369e0878cb9c8", "extraction_time": 1638357863.4242253, "agent_name": "Your Name", "agent_email": "you@example.com", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"@context": {"@vocab": "http://schema.org/", "datalad": "http://dx.datalad.org/"}, "@graph": [{"@id": "59286713dacabfbce1cecf4c865fff5a", "@type": "agent", "name": "Your Name", "email": "you@example.com"}, {"@id": "bcf9cfde4a599d26094a58efbe4369e0878cb9c8", "identifier": "ceeb844a-c6e8-4b2f-bb7c-62b7ae449a9f", "@type": "Dataset", "version": "0-4-gbcf9cfd", "dateCreated": "2021-12-01T12:24:17+01:00", "dateModified": "2021-12-01T12:24:19+01:00", "hasContributor": {"@id": "59286713dacabfbce1cecf4c865fff5a"}}]}}
   {"type": "file", "path": "file1.txt", "dataset_id": "ceeb844a-c6e8-4b2f-bb7c-62b7ae449a9f", "dataset_version": "bcf9cfde4a599d26094a58efbe4369e0878cb9c8", "extraction_time": 1638357864.5259314, "agent_name": "Your Name", "agent_email": "you@example.com", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"@id": "datalad:SHA1-s13--2ef267e25bd6c6a300bb473e604b092b6a48523b", "contentbytesize": 13}}
   {"type": "file", "path": "file.bin", "dataset_id": "ceeb844a-c6e8-4b2f-bb7c-62b7ae449a9f", "dataset_version": "bcf9cfde4a599d26094a58efbe4369e0878cb9c8", "extraction_time": 1638357864.5327883, "agent_name": "Your Name", "agent_email": "you@example.com", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"@id": "datalad:MD5E-s512000--816df6f64deba63b029ca19d880ee10a.bin", "contentbytesize": 512000}}
   {"type": "dataset", "root_dataset_id": "<unknown>", "root_dataset_version": "7228f027171f7b8949a47812a651600412f2577e", "dataset_path": "subds1", "dataset_id": "4e3422f4-b606-4cf9-818a-a3bb840e3396", "dataset_version": "ddf2a2758fd6773a1171a6fbae4afe48cc982773", "extraction_time": 1638357869.7052076, "agent_name": "Your Name", "agent_email": "you@example.com", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"@context": {"@vocab": "http://schema.org/", "datalad": "http://dx.datalad.org/"}, "@graph": [{"@id": "59286713dacabfbce1cecf4c865fff5a", "@type": "agent", "name": "Your Name", "email": "you@example.com"}, {"@id": "ddf2a2758fd6773a1171a6fbae4afe48cc982773", "identifier": "4e3422f4-b606-4cf9-818a-a3bb840e3396", "@type": "Dataset", "version": "0-3-gddf2a27", "dateCreated": "2021-12-01T12:24:25+01:00", "dateModified": "2021-12-01T12:24:27+01:00", "hasContributor": {"@id": "59286713dacabfbce1cecf4c865fff5a"}}]}}
   {"type": "file", "path": "file_subds1.1.txt", "root_dataset_id": "<unknown>", "root_dataset_version": "7228f027171f7b8949a47812a651600412f2577e", "dataset_path": "subds1", "dataset_id": "4e3422f4-b606-4cf9-818a-a3bb840e3396", "dataset_version": "ddf2a2758fd6773a1171a6fbae4afe48cc982773", "extraction_time": 1638357868.706351, "agent_name": "Your Name", "agent_email": "you@example.com", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"@id": "datalad:SHA1-s36--9ce18068eb4126c23235d965c179b2a53546d104", "contentbytesize": 36}}


.. todo::

   Upcoming: how to delete metadata, how to filter metadata, and how to export metadata.
