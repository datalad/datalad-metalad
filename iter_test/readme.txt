Some examples for iterator use with external tools
The examples below use a very simple adapator, that emits one argument per line.
There is no separator-argument, so the downstream tools all convert a fixed number
of lines, i.e. 6, into program arguments


# The following two examples are on longnow-podcast dataset, which contains 484 files

# Use parallel to extract metadata from files that are delivered by an iterator
# This finished in (on cm's machine) real 1:52, user: 5:57
./iter_test/iterate_dataset.py ~/datalad/longnow-podcasts file False|./iter_test/extract_adaptor.py metalad_core|parallel -j 8 -N 6 datalad meta-extract


# A serial execution of extraction with xargs (requirest changing the xarg-argumnet delimiter to '\n'.
# This finished in (on cm's machine) real 5:49, user: 5:03
./iter_test/iterate_dataset.py ~/datalad/longnow-podcasts file False|./iter_test/extract_adaptor.py metalad_core|xargs -d '\n' -n 6 datalad meta-extract
