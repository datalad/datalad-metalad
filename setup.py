#!/usr/bin/env python

import sys
from setuptools import setup

import versioneer

from _datalad_buildsupport.setup import (
    BuildManPage,
)

cmdclass = versioneer.get_cmdclass()
cmdclass.update(build_manpage=BuildManPage)

# Give setuptools a hint to complain if it's too old a version
# 30.3.0 allows us to put most metadata in setup.cfg
# Should match pyproject.toml
SETUP_REQUIRES = ['setuptools >= 30.3.0']

# This enables setuptools to install wheel on-the-fly
SETUP_REQUIRES += ['wheel'] if 'bdist_wheel' in sys.argv else []


setup(
    name="datalad_metalad",
    version=versioneer.get_version(),
    cmdclass=cmdclass,
    setup_requires=SETUP_REQUIRES,
    packages=[
        'datalad_metalad',
        'datalad_metalad.pipeline',
        'datalad_metalad.pipeline.consumer',
        'datalad_metalad.pipeline.pipelines',
        'datalad_metalad.pipeline.processor',
        'datalad_metalad.pipeline.provider',
        'datalad_metalad.extractors',
        'datalad_metalad.extractors.tests',
        'datalad_metalad.extractors.legacy',
        'datalad_metalad.extractors.legacy.tests',
        'datalad_metalad.extractors.studyminimeta',
        'datalad_metalad.filters',
        'datalad_metalad.indexers',
        'datalad_metalad.indexers.tests',
        'datalad_metalad.pathutils',
        'datalad_metalad.pathutils.tests',
        'datalad_metalad.tests',
        'datalad_metalad.metadatatypes',
        '_datalad_buildsupport',
    ],
    package_dir={
        'datalad_metalad.pipeline': 'datalad_metalad/pipeline',
    },
    package_data={
        'datalad_metalad.pipeline': ['pipelines/*.json'],
        'datalad_metalad.extractors.legacy.tests': ['data/*'],
    },
    entry_points={
        'datalad.extensions': [
            'metalad=datalad_metalad:command_suite',
        ],
        'datalad.tests': [
            'metalad=datalad_metalad'
        ],
        'datalad.metadata.extractors': [
            'metalad_core=datalad_metalad.extractors.core:DataladCoreExtractor',
            'metalad_example_dataset=datalad_metalad.extractors.metalad_example_dataset:MetaladExampleDatasetExtractor',
            'metalad_example_file=datalad_metalad.extractors.metalad_example_file:MetaladExampleFileExtractor',
            'metalad_external_dataset=datalad_metalad.extractors.external_dataset:ExternalDatasetExtractor',
            'metalad_external_file=datalad_metalad.extractors.external_file:ExternalFileExtractor',
            'metalad_annex=datalad_metalad.extractors.annex:AnnexMetadataExtractor',
            'metalad_custom=datalad_metalad.extractors.custom:CustomMetadataExtractor',
            'metalad_runprov=datalad_metalad.extractors.runprov:RunProvenanceExtractor',
            'metalad_studyminimeta=datalad_metalad.extractors.studyminimeta.main:StudyMiniMetaExtractor',
            'external_dataset=datalad_metalad.extractors.external_dataset:ExternalDatasetExtractor',
            'external_file=datalad_metalad.extractors.external_file:ExternalFileExtractor',
            'annex=datalad_metalad.extractors.legacy.annex:AnnexMetadataExtractor',
            'audio=datalad_metalad.extractors.legacy.audio:AudioMetadataExtractor',
            'datacite=datalad_metalad.extractors.legacy.datacite:DataciteMetadataExtractor',
            'datalad_core=datalad_metalad.extractors.legacy.datalad_core:DataladCoreMetadataExtractor',
            'datalad_rfc822=datalad_metalad.extractors.legacy.datalad_rfc822:DataladRFC822MetadataExtractor',
            'exif=datalad_metalad.extractors.legacy.exif:ExifMetadataExtractor',
            'frictionless_datapackage=datalad_metalad.extractors.legacy.frictionless_datapackage:FRDPMetadataExtractor',
            'image=datalad_metalad.extractors.legacy.image:ImageMetadataExtractor',
            'xmp=datalad_metalad.extractors.legacy.xmp:XmpMetadataExtractor',
        ],
        'datalad.metadata.indexers': [
            'metalad_studyminimeta=datalad_metalad.indexers.studyminimeta:StudyMiniMetaIndexer',
        ],
        'datalad.metadata.filters': [
            'metalad_demofilter=datalad_metalad.filters.demofilter:DemoFilter',
        ],
    },
)
