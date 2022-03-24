
.. -*- mode: rst -*-
.. vi: set ft=rst sts=4 ts=4 sw=4 et tw=79:

.. _chap_aggregation:


***********
Aggregation
***********

.. topic:: Specification scope and status

   This specification provides an overview over meta-aggregate concepts and
   its current implementation.

Purpose and Design
==================

Meta-aggregate allows to collect metadata that is stored in metadata-stores of sub-datasets into the metadata-store of the super-dataset.

Challenges
..........

In git a version of a repository, i.e. a commit-hash, determines the versions of the sub-modules. Metalad uses a similar concept to relate sub-datasets to datasets. Since datalad datasets are git-repository with some additional information, we decided that aggregated sub-dataset metadata is available under the sub-dataset path in a certain root-dataset version.

Example: in root-dataset "R" version "100" there is a subdataset "S" at ./d1/sub1 with version "300". (For our examples we assume that version number are consecutive)

But that does ont mean, that metadata in d1/sub1 was extracted in sub-dataset version "300". It might have been extracted earlier, e.g. in version "280". The metadata carries the version of the primary data. Since sub-dataset paths can change, we do not know which relation existed between "R" and "S". Maybe there was a different path, or maybe they were not related at all because "S" was not a sub-dataset of "R" until thw version "290" of "S". So even a complete search through all existing versions would not guarantee that we could find a version of "R" where "S" in version "280" is some sub-dataset with a sub-dataset-path. We know that "S" is at "d1/sub1" in "R" version "300".


Because the metadata information is tied to the version (commit-hash) of the dataset at its extraction, and because sub-dataset-metadata containment is represented by the dataset-tree. There are a few challenges.

Let us assume that there is a root-dataset "." with id "R" and version "1" and a sub-dataset at "./d1/d1.1/sub1" with id "S" and version "300". Now the following events happen:

1. We extract in the sub-dataset at version "300", resulting in metadata(S, 300)

2. We update the root dataset to version "2".

3. We extract in the root-dataset at version "2", resulting in metadata(R, 2)

4. We update the root dataset to version "3".

5. We update the sub-dataset to version "301", and the root-dataset to version "4".

6. We extract in the sub-dataset at version "301", resulting in metadata(S, 301)


So we have the following metadata in the root-dataset
- metadata(R, 2)

We have the following metadata in the sub-dataset
- metadata(S, 300)
- metadata(S, 301)

The current file-tree is:
.@4--->d1/d.1.1/sub1@301

If we want to aggregate the d1/d1.1/sub1 into the root metadata store:

We can tell that metadata(R, 2) belongs to: .@2

We can also tell that metadata(S, 301) belongs to: .[@2]/d1/d1.1/sub1[@301]. So we can store this metadata at: .[@2]/d1/d1.1/sub1

We cannot tell (easily) where metadata(S, 300) belongs to. Unless we find the relation .[@1]/d1/d1.1/sub1[@300]. This does not even have to exist, because the path might have changed.

So we know that something with the ID "S" and the current path of "d1/d1.1/sub1" has metadata extracted when it was in version "300". We do not know anymore whether version "300" of "S" was ever connected to "." and, if it was, under which path.

