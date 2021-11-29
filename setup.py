#!/usr/bin/env python

import sys
from setuptools import setup
import versioneer

# Give setuptools a hint to complain if it's too old a version
# 30.3.0 allows us to put most metadata in setup.cfg
# Should match pyproject.toml
SETUP_REQUIRES = ['setuptools >= 30.3.0']
# This enables setuptools to install wheel on-the-fly
SETUP_REQUIRES += ['wheel'] if 'bdist_wheel' in sys.argv else []

setup(
    name="datalad_metalad",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    setup_requires=SETUP_REQUIRES,
    packages=['datalad_metalad',
              'datalad_metalad.extractors',
              'datalad_metalad.indexers',
              'datalad_metalad.pathutils',
              'datalad_metalad.processor',
              'datalad_metalad.provider',
              'datalad_metalad.tests'],
    package_data={'datalad_metalad': ['pipelines/*.json']},
    entry_points={
        'datalad.extensions': [
            'metalad=datalad_metalad:command_suite',
        ],
        'datalad.tests': [
            'metalad=datalad_metalad'
        ],
        'datalad.metadata.extractors': [
            'metalad_core=datalad_metalad.extractors.core:DataladCoreExtractor',
            'metalad_core_dataset=datalad_metalad.extractors.core_dataset:DataladCoreDatasetExtractor',
            'metalad_core_file=datalad_metalad.extractors.core_file:DataladCoreFileExtractor',
            'metalad_external_dataset=datalad_metalad.extractors.external_dataset:ExternalDatasetExtractor',
            'metalad_external_file=datalad_metalad.extractors.external_file:ExternalFileExtractor',
            'metalad_annex=datalad_metalad.extractors.annex:AnnexMetadataExtractor',
            'metalad_custom=datalad_metalad.extractors.custom:CustomMetadataExtractor',
            'metalad_runprov=datalad_metalad.extractors.runprov:RunProvenanceExtractor',
            'metalad_studyminimeta=datalad_metalad.extractors.studyminimeta.main:StudyMiniMetaExtractor',
            'external_dataset=datalad_metalad.extractors.external_dataset:ExternalDatasetExtractor',
            'external_file=datalad_metalad.extractors.external_file:ExternalFileExtractor',
        ],
        'datalad.metadata.indexers': [
            'metalad_studyminimeta=datalad_metalad.indexers.studyminimeta:StudyMiniMetaIndexer',
        ]
    },
)
