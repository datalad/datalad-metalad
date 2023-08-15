Some examples for iterator use with external tools
The examples below use a very simple adapator, that emits one argument per line.
There is no separator-argument, so the downstream tools all convert a fixed number
of lines, i.e. 8, into program arguments


# The following examples are on longnow-podcast dataset, which contains 484 files

# Use parallel to extract metadata from files that are delivered by an iterator
# This finished in (on cm's machine) real 01:44, user: 05:46
./iter_test/iterate_dataset.py ~/datalad/longnow-podcasts file False|./iter_test/extract_adaptor.py metalad_core|parallel -j 8 -N 8 datalad meta-extract


# A serial execution of extraction with xargs (requirest changing the xarg-argumnet delimiter to '\n'.
# This finished in (on cm's machine) real 05:49, user: 05:03
./iter_test/iterate_dataset.py ~/datalad/longnow-podcasts file False|./iter_test/extract_adaptor.py metalad_core|xargs -d '\n' -n 8 datalad meta-extract


# Adding metadata with the following command line:
# This executes (on cm's machine with 4 cores) in: real: 01:47, user: 05:45
./iter_test/iterate_dataset.py ~/datalad/longnow-podcasts file False \
    |./iter_test/extract_file_adaptor.py metalad_core \
    |parallel -N 8 datalad meta-extract \
    |datalad meta-add -i -d ~/datalad/longnow-podcasts --json-lines -


# The following adds about 1000 file metadata entries per minute (on lad1 with 64 cores)
./iter_test/iterate_dataset.py /playground/cristian/datalad/datasets.datalad.org/labs/haxby file True \
    |./iter_test/extract_file_adaptor.py metalad_core \
    |parallel -N 8 datalad meta-extract \
    |datalad meta-add -d /playground/cristian/datalad/datasets.datalad.org/labs/haxby -i --json-lines -


# This executes (on cm's machine) in: real: 01:47, user: 05:45
./iter_test/iterate_dataset.py ~/datalad/longnow-podcasts dataset False \
    |./iter_test/extract_dataset_adaptor.py metalad_core \
    |parallel -N 7 datalad meta-extract \
    |datalad meta-add -i -d ~/datalad/longnow-podcasts --json-lines -
