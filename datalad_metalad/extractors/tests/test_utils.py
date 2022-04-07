# -*- coding: utf-8 -*-
from datalad.utils import make_tempfile
from nose.tools import assert_raises
from ..utils import get_text_from_file

PLAIN_TEXT="""Lorem ipsum dolor sit amet, consectetur adipiscing elit. Quisque quam quam,
ultrices vel suscipit ut, aliquam id tortor. Aliquam egestas mauris finibus
nisl convallis, et hendrerit nunc pharetra. Quisque ut lacus at lacus ultrices
porttitor at vel libero. Ut ante dolor, placerat ac neque at, molestie sagittis
mi.

    Morbi eget ipsum elementum, consectetur nulla at, molestie odio. Donec
varius diam massa, non suscipit lorem ultrices ut. Sed eu cursus libero. In
commodo, massa eu molestie gravida, dolor ante luctus sapien, ut pretium tellus
nisl et nunc. Vestibulum condimentum, arcu sit amet hendrerit sagittis, ante dui
accumsan elit, a tincidunt augue est id ligula. Curabitur cursus eros et varius
gravida. Suspendisse id aliquet massa. Vestibulum massa leo, euismod eu
ullamcorper quis, gravida eget est."""

MARKDOWN_README="""# The studyforrest.org Dataset

[![made-with-datalad](https://www.datalad.org/badges/made_with.svg)](https://datalad.org)
[![PDDL-licensed](https://img.shields.io/badge/license-PDDL-blue.svg)](http://opendatacommons.org/licenses/pddl/summary)
[![No registration or authentication required](https://img.shields.io/badge/data_access-unrestricted-green.svg)]()
[![doi](https://img.shields.io/badge/doi-missing-lightgrey.svg)](http://dx.doi.org/)

For further information about the project visit: http://studyforrest.org

## Content

- ``artifact/``

  Pristine data artifacts for all acquisitions. Not publicly accessible.

- ``code/``

  Code for data structuring, tests, conversion, and analysis.

- ``derivative/``

  Preprocessed data, and analysis results.

- ``original/``

  Raw, or minimally processed data.

- ``stimulus/``

  Annotations of the Forrest Gump movie stimulus and its audio description,
  from human observers and computational algorithms.


## How to obtain the data files

This repository is a [DataLad](https://www.datalad.org/) dataset. It provides
fine-grained data access down to the level of individual files, and allows for
tracking future updates. In order to use this repository for data retrieval,
[DataLad](https://www.datalad.org/) is required. It is a free and
open source command line tool, available for all major operating
systems, and builds up on Git and [git-annex](https://git-annex.branchable.com/)
to allow sharing, synchronizing, and version controlling collections of
large files. You can find information on how to install DataLad at
[handbook.datalad.org/intro/installation.html](http://handbook.datalad.org/intro/installation.html).

### Get the dataset

A DataLad dataset can be `cloned` by running

```
datalad clone <url>
```

Once a dataset is cloned, it is a light-weight directory on your local machine.
At this point, it contains only small metadata and information on the
identity of the files in the dataset, but not actual *content* of the
(sometimes large) data files.

### Retrieve dataset content

After cloning a dataset, you can retrieve file contents by running

```
datalad get <path/to/directory/or/file>
```

This command will trigger a download of the files, directories, or
subdatasets you have specified.

DataLad datasets can contain other datasets, so called *subdatasets*.
If you clone the top-level dataset, subdatasets do not yet contain
metadata and information on the identity of files, but appear to be
empty directories. In order to retrieve file availability metadata in
subdatasets, run

```
datalad get -n <path/to/subdataset>
```

Afterwards, you can browse the retrieved metadata to find out about
subdataset contents, and retrieve individual files with `datalad get`.
If you use `datalad get <path/to/subdataset>`, all contents of the
subdataset will be downloaded at once.

### Stay up-to-date

DataLad datasets can be updated. The command `datalad update` will
*fetch* updates and store them on a different branch (by default
`remotes/origin/master`). Running

```
datalad update --merge
```

will *pull* available updates and integrate them in one go.

### More information

More information on DataLad and how to use it can be found in the DataLad Handbook at
[handbook.datalad.org](http://handbook.datalad.org/en/latest/index.html). The chapter
"DataLad datasets" can help you to familiarize yourself with the concept of a dataset."""

all_content = [
    PLAIN_TEXT,
    MARKDOWN_README
]

all_files = [
    {
        "name": "readme.txt",
        "content": PLAIN_TEXT
    },
    {
        "name": "README.md",
        "content": MARKDOWN_README
    }
]

def test_get_text_from_file():
    # Test all file content
    for content_string in all_content:
        with make_tempfile(content=content_string) as fname:
            assert get_text_from_file(fname) == content_string
    # Test raise exception for non-existent file
    nonfile = 'thisfiledoesnotexist.ipynb'
    assert_raises(FileNotFoundError, get_text_from_file, nonfile)
