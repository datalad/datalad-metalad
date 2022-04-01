# DataLad extension for semantic metadata handling

[![Build_status](https://ci.appveyor.com/api/projects/status/hlwg6yi008mbmr1m?svg=true)](https://ci.appveyor.com/project/mih/datalad-metalad) [![codecov.io](https://codecov.io/github/datalad/datalad-metalad/coverage.svg?branch=master)](https://codecov.io/github/datalad/datalad-metalad?branch=master) [![GitHub release](https://img.shields.io/github/release/datalad/datalad-metalad.svg)](https://GitHub.com/datalad/datalad-metalad/releases/) [![PyPI version fury.io](https://badge.fury.io/py/datalad-metalad.svg)](https://pypi.python.org/pypi/datalad-metalad/) [![Documentation](https://readthedocs.org/projects/datalad-metalad/badge/?version=latest)](http://docs.datalad.org/projects/metalad/en/latest)


### Overview

This software is a [DataLad](http://datalad.org) extension that equips DataLad
with an alternative command suite for metadata handling (extraction, aggregation,
filtering, and reporting).

Please note that the metadata storage format introduced in release 0.3.0 is incompatible
with the metadata storage formate in previous versions, i.e. `0.2.x`, and in DataLad
proper. They both happily coexist on storage, but this version of metalad will not
be able to read metadata that was stored by the previous version and vice versa.
Eventually there will be an importer that will pull old-version metadata into
the new metadata storage. It is planned for release 0.3.1

Here is an overview of the changes in 0.3.0 (the new system is quite
different from the previous release in a few ways):

1. Leaner commands with unix-style behavior, i.e. one command for one operation, and commands are chainable (use results from one command as input for another command, e.g. meta-extract|meta-add).

2. MetadataRecord modifications does not alter the state of the datalad dataset. In previous releases, changes to metadata have altered the version (commit-hash) of the repository although the primary data did not change. This is not the case in the new system. The new system does provide information about the primary data version, i.e. commit-hash, from which the individual metadata elements were created.

3. The ability to support a wide range of metadata storage backends in the future (this is facilitated by the [datalad-metadata-model](https://github.com/datalad/metadata-model)) which is developed alongside metalad), which separates the logical metadata model used in metalad from the storage backends, by abstracting the storage backend), Currently git-repository storage is supported.

4. The ability to transport metadata independently of the data in the dataset. The new system introduces the concept of a *metadata-store* which is usually the git-repository of the datalad dataset that is described by the metadata. But this is not a mandatory configuration, metadata can be stored in almost any git-repository.

5. The ability to report a subset of metadata from a remote metadata store without downloading the complete remote metadata. In fact only the minimal necessary information is transported from the remote metadata store. This ability is available to all metadata-based operations, for example, also to filtering.

6. A new simplified extractor model that distinguishes between two extractor-types: dataset-level extractors and file-extractors. The former are executed with a view on a dataset, the latter are executed with specific information about a single file-path in the dataset. The previous extractors (datalad, and datalad-metalad<=0.2.1) are still supported.

7. A built-in pipeline mechanism that allows parallel execution of metadata operations like metadata extraction, and metadata filtering. (Still in early stage)

8. A new set of commands that allow operations that map metadata to metadata. Those operations are called filtering and are implemented by MetadataFilter-classes. Filter are dynamically loaded and custom filter are supports, much like extractors. (Still in early stage)

9. Backward compatibility supporting an import from previous metadata storage (planned for 0.3.1).


Command(s) currently provided by this extension

- `meta-extract` -- run an extractor on a file or dataset and emit the 
resulting metadata (stdout).

- `meta-filter` -- run an filter over existing metadata and return the
resulting metadata (stdout).

- `meta-add` -- add a metadata record or a list of metadata records
(possibly received on stdin) to a metadata store, usually to the git-repo of the dataset.

- `meta-aggregate` -- aggregate metadata from multiple local or remote
metadata-stores into a local metadata store.

- `meta-dump` -- reporting metadata from local or remote metadata stores. Allows
to select metadata by file- or dataset-path matching patterns including
dataset versions and dataset IDs. 

- `meta conduct` -- execute processing pipelines that consist of a provider
which emits objects that should be processed, e.g. files or metadata, and
a pipeline of processors, that perform operations on the provided objects,
such as metadata-extraction and metadata-adding.Processors
are usually executed in parallel. A few pipeline definitions are provided
with the release.

Commands currently under development:

- `meta-export` -- write a flat representation of metadata to a file-system. For now you
  can export your metadata to a JSON-lines file named `metadata-dump.jsonl`:
    ```
     datalad meta-dump -d <dataset-path> -r >metadata-dump.jsonl
    ```

- `meta-import` -- import a flat representation of metadata from a file-system. For now you 
   can import metadata from a JSON-lines file, e.g.  `metadata-dump.jsonl` like this:
    ```
     datalad meta-add -d <dataset-path> --json-lines -i metadata-dump.jsonl
    ```

- `meta-ingest-previous` -- ingest metadata from `metalady<=0.2.1`.


*A word of caution: documentation is still lacking and will be addressed with release 0.3.1.*


Additional metadata extractor implementations

- Compatible with the previous families of extractors provided by datalad
and by metalad, i.e. `metalad_core`, `metalad_annex`, `metalad_custom`, `metalad_runprov`
 
- New metadata extractor paradigm that distinguishes between file- and
dataset-level extractors. Included are two example extractors, `metalad_example_dataset`, 
and `metalad_example_file`

- `metalad_external_dataset` and `metalad_external_file`, a dataset- and a
file-extractors that execute external processes to generate metadata allow
processing of the externally created metadata in datalad.

- `metalad_studyminimeta` -- a dataset-level extractor that reads studyminimeta yaml
files and produces metadata that contains a JSON-LD compatible description of the 
data in the input file



Indexers

- Provides indexers for the new datalad indexer-plugin interface. These indexers
convert metadata in proprietary formats into a set of key-value pairs that can
be used by `datalad search` to search for content.

- `indexer_studyminimeta` -- converts studyminimeta JSON-LD description into
key-value pairs for `datalad search`.

- `indexer_jsonld` -- a generic JSON-LD indexer that aims at converting any 
JSON-LD descriptions into a set of key-value pairs that reflect the content of the
JSON-LD description.


## Installation

Before you install this package, please make sure that you [install a recent
version of git-annex](https://git-annex.branchable.com/install).  Afterwards,
install the latest version of `datalad-metalad` from
[PyPi](https://pypi.org/project/datalad-metalad). It is recommended to use
a dedicated [virtualenv](https://virtualenv.pypa.io):

    # create and enter a new virtual environment (strongly recommended)
    virtualenv --system-site-packages --python=python3 ~/env/datalad
    . ~/env/datalad/bin/activate

    # install from github
    pip install datalad-metalad


## Support

For general information on how to use or contribute to DataLad (and this
extension), please see the [DataLad website](http://datalad.org) or the
[main GitHub project page](http://datalad.org). The documentation is found
here: http://docs.datalad.org/projects/metalad

All bugs, concerns and enhancement requests for this software can be submitted here:
https://github.com/datalad/datalad-metalad/issues

If you have a problem or would like to ask a question about how to use DataLad,
please [submit a question to
NeuroStars.org](https://neurostars.org/tags/datalad) with a ``datalad`` tag.
NeuroStars.org is a platform similar to StackOverflow but dedicated to
neuroinformatics.

All previous DataLad questions are available here:
http://neurostars.org/tags/datalad/

## Acknowledgements

This DataLad extension was developed with support from the German Federal
Ministry of Education and Research (BMBF 01GQ1905), and the US National Science
Foundation (NSF 1912266).
