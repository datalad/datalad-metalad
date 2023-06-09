An example for iterator use with parallel execution. The example uses the 
extract adapter with argumnets (extract_ad_arg) to extract metadata from all
the results of the iterator:

tools/iter_test/iterate_dataset.py ~/datalad/longnow-podcasts both False|parallel -j 10 tools/iter_test/extract_ad_arg.py metalad_core
