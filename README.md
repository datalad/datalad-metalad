# [Unstable] DataLad extension for semantic metadata handling

[![Build_status](https://ci.appveyor.com/api/projects/status/hlwg6yi008mbmr1m?svg=true)](https://ci.appveyor.com/project/mih/datalad-metalad) [![codecov.io](https://codecov.io/github/datalad/datalad-metalad/coverage.svg?branch=master)](https://codecov.io/github/datalad/datalad-metalad?branch=master) [![GitHub release](https://img.shields.io/github/release/datalad/datalad-metalad.svg)](https://GitHub.com/datalad/datalad-metalad/releases/) [![PyPI version fury.io](https://badge.fury.io/py/datalad-metalad.svg)](https://pypi.python.org/pypi/datalad-metalad/) [![Documentation](https://readthedocs.org/projects/datalad-metalad/badge/?version=latest)](http://docs.datalad.org/projects/metalad/en/latest)

**NB! This software is currently under heavy re-writing. This includes the master
branch, which is currently considered unstable.**

This software is a [DataLad](http://datalad.org) extension that equips DataLad
with an alternative command suite for metadata handling (extraction, aggregation,
reporting).

Please note that the metadata storage format used in this version is incompatible
with the metadata storage formate in previous versions, i.e. `0.2.x`, and in DataLad
proper.

This version stores metadata in git-repositories. By default the git repository in
which the dataset is stored is used to store metadata.
 
Command(s) currently provided by this extension

- `meta-extract` -- run an extractor on a file or dataset and store the
resulting metadata in a local metadata store, i.e. in the dataset git-repo
- `meta-add` -- add a metadata record (or a list of metadata records) to a
metadata store, usually to the git-repo of the dataset.
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

Additional metadata extractor implementations

- Compatibel with the previous families of extractors provided by datalad
and by metalad, i.e. `metalad_core`, `metalad_annex`, `metalad_custom`, `metalad_runprov`
 
- New metadata extractor paradigm that distinguishes between file- and
dataset-level extractors.

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

    # create and enter a new virtual environment (optional)
    virtualenv --system-site-packages --python=python3 ~/env/datalad
    . ~/env/datalad/bin/activate

    # install from PyPi
    pip install datalad_metalad


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
