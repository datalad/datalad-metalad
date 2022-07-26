# -*- coding: utf-8 -*-
from pathlib import Path
from unittest import mock

from datalad.api import meta_extract
from datalad.distribution.dataset import Dataset
from datalad.tests.utils_pytest import (
    with_tempfile,
    with_tree
)

from ..studyminimeta.ldcreator import LDCreator
from ..studyminimeta.main import StudyMiniMetaExtractor


STUDYMINIMETA_PERSONS = {
    "a@example.com": {
        "given_name": "Alex",
        "last_name": "Meyer",
        "orcid-id": "0000-0001-0002-0033",
        "title": "Prof. Dr.",
        "contact_information": "Corner broadway and main",
        "affiliation": "Big Old University"
    },
    "b@example.com": {
        "given_name": "Bernd",
        "last_name": "Muller",
        "orcid-id": "0000-0001-0002-0034",
        "title": "MD",
        "contact_information": "central plaza",
        "affiliation": "RRU-SSLT"
    }
}


STUDYMINIMETA_STUDY = {
    "name": "An example study",
    "purpose": "a short description of the study",
    "principal_investigator": "a@example.com",
    "keyword": ["k1", "k2"],
    "funding": ["DFG", "NHI"],
    "contributor": ["a@example.com", "b@example.com"],
    "contact_information": "Corner broadway and main",
    "start_date": "01.01.2020",
    "end_date": "02.02.2020"
}


STUDYMINIMETA_PUBLICATIONS = [
    {
        "title": "Publication Numero Unos",
        "year": 2001,
        "author": ["a@example.com", "b@example.com"],
        "doi": "doi:1112-23232-0",
        "volume": 30,
        "issue": 4,
        "pages": "1-1200",
        "publisher": "Renato"
    },
    {
        "title": "Publication Numero Dos",
        "year": 2002,
        "author": ["a@example.com", "b@example.com"],
        "doi": "doi:1112-23232-1",
        "volume": 400,
        "publisher": "Vorndran"
    },
    {
        "title": "Publication Numero Tres",
        "year": 2003,
        "author": ["a@example.com", "b@example.com"],
        "doi": "doi:1112-23232-2",
        "issue": 500,
        "publisher": "Eisenberg"
    },
    {
        "title": "Publication Numero Quatro",
        "year": 2004,
        "author": ["a@example.com", "b@example.com"],
        "doi": "doi:1112-23232-3",
        "publisher": "Killian-Tumler"
    }
]


STUDYMINIMETA_DATASET = {
    "name": "Dataset for an example study",
    "location": "http://example.com/datalad/example_study",
    "description": "Data that was collected from a source in order to prove something",
    "author": [
        "a@example.com",
        "b@example.com"
    ],
    "keywords": ["d_k1", "d_k2", "d_k3"],
    "standard": ["dicom", "ebdsi"],
    "funding": ["DFG", "NHI", "Microsoft"]
}


STUDYMINIMETA_METADATA_MAXIMAL_SPEC = {
    "study": STUDYMINIMETA_STUDY,
    "dataset": STUDYMINIMETA_DATASET,
    "person": STUDYMINIMETA_PERSONS,
    "publication": STUDYMINIMETA_PUBLICATIONS
}


EXPECTED_MAXIMAL_JSON_LD = {
    "@context": {
        "@vocab": "http://schema.org/"
    },
    "@graph": [
        {
            "@id": "#study",
            "@type": "CreativeWork",
            "name": "An example study",
            "abstract": "a short description of the study",
            "accountablePerson": "a@example.com",
            "keywords": [
                "k1",
                "k2"
            ],
            "dateCreated": "01.01.2020",
            "description": "end_date: 02.02.2020",
            "contributor": [
                {
                    "@id": "https://schema.datalad.org/person#a@example.com"
                },
                {
                    "@id": "https://schema.datalad.org/person#b@example.com"
                }
            ],
            "funder": [
                {
                    "@id": "https://schema.datalad.org/organization#DFG",
                    "@type": "Organization",
                    "name": "DFG"
                },
                {
                    "@id": "https://schema.datalad.org/organization#NHI",
                    "@type": "Organization",
                    "name": "NHI"
                }
            ]
        },
        {
            "@id": "https://schema.datalad.org/datalad_dataset#id-0000--0000",
            "@type": "Dataset",
            "version": "a02312398324778972389472834",
            "name": "Dataset for an example study",
            "url": "http://example.com/datalad/example_study",
            "description": "Data that was collected from a source in order to prove something",
            "author": [
                {
                    "@id": "https://schema.datalad.org/person#a@example.com"
                },
                {
                    "@id": "https://schema.datalad.org/person#b@example.com"
                }
            ],
            "funder": [
                {
                    "@id": "https://schema.datalad.org/organization#DFG",
                    "@type": "Organization",
                    "name": "DFG"
                },
                {
                    "@id": "https://schema.datalad.org/organization#NHI",
                    "@type": "Organization",
                    "name": "NHI"
                },
                {
                    "@id": "https://schema.datalad.org/organization#Microsoft",
                    "@type": "Organization",
                    "name": "Microsoft"
                }
            ],
            "hasPart": {
                "@id": "#standards",
                "@type": "DefinedTermSet",
                "hasDefinedTerm": [
                    {
                        "@id": "https://schema.datalad.org/standard#dicom",
                        "@type": "DefinedTerm",
                        "termCode": "dicom"
                    },
                    {
                        "@id": "https://schema.datalad.org/standard#ebdsi",
                        "@type": "DefinedTerm",
                        "termCode": "ebdsi"
                    }
                ]
            }
        },
        {
            "@id": "#personList",
            "@list": [
                {
                    "@id": "https://schema.datalad.org/person#a@example.com",
                    "@type": "Person",
                    "email": "a@example.com",
                    "name": "Prof. Dr.  Alex Meyer",
                    "givenName": "Alex",
                    "familyName": "Meyer",
                    "sameAs": "0000-0001-0002-0033",
                    "honorificSuffix": "Prof. Dr.",
                    "affiliation": "Big Old University",
                    "contactPoint": {
                        "@id": "#contactPoint(a@example.com)",
                        "@type": "ContactPoint",
                        "description": "Corner broadway and main"
                    }
                },
                {
                    "@id": "https://schema.datalad.org/person#b@example.com",
                    "@type": "Person",
                    "email": "b@example.com",
                    "name": "MD  Bernd Muller",
                    "givenName": "Bernd",
                    "familyName": "Muller",
                    "sameAs": "0000-0001-0002-0034",
                    "honorificSuffix": "MD",
                    "affiliation": "RRU-SSLT",
                    "contactPoint": {
                        "@id": "#contactPoint(b@example.com)",
                        "@type": "ContactPoint",
                        "description": "central plaza"
                    }
                }
            ]
        },
        {
            "@id": "#publicationList",
            "@list": [
                {
                    "@id": "#publication[0]",
                    "@type": "ScholarlyArticle",
                    "headline": "Publication Numero Unos",
                    "datePublished": 2001,
                    "sameAs": "doi:1112-23232-0",
                    "pagination": "1-1200",
                    "author": [
                        {
                            "@id": "https://schema.datalad.org/person#a@example.com"
                        },
                        {
                            "@id": "https://schema.datalad.org/person#b@example.com"
                        }
                    ],
                    "publisher": {
                        "@id": "https://schema.datalad.org/publisher#Renato",
                        "@type": "Organization",
                        "name": "Renato"
                    },
                    "isPartOf": {
                        "@id": "#issue(4)",
                        "@type": "PublicationIssue",
                        "issue_number": 4,
                        "isPartOf": {
                            "@id": "#volume(30)",
                            "@type": "PublicationVolume",
                            "volumeNumber": 30
                        }
                    }
                },
                {
                    "@id": "#publication[1]",
                    "@type": "ScholarlyArticle",
                    "headline": "Publication Numero Dos",
                    "datePublished": 2002,
                    "sameAs": "doi:1112-23232-1",
                    "author": [
                        {
                            "@id": "https://schema.datalad.org/person#a@example.com"
                        },
                        {
                            "@id": "https://schema.datalad.org/person#b@example.com"
                        }
                    ],
                    "publisher": {
                        "@id": "https://schema.datalad.org/publisher#Vorndran",
                        "@type": "Organization",
                        "name": "Vorndran"
                    },
                    "isPartOf": {
                        "@id": "#volume(400)",
                        "@type": "PublicationVolume",
                        "volumeNumber": 400
                    }
                },
                {
                    "@id": "#publication[2]",
                    "@type": "ScholarlyArticle",
                    "headline": "Publication Numero Tres",
                    "datePublished": 2003,
                    "sameAs": "doi:1112-23232-2",
                    "author": [
                        {
                            "@id": "https://schema.datalad.org/person#a@example.com"
                        },
                        {
                            "@id": "https://schema.datalad.org/person#b@example.com"
                        }
                    ],
                    "publisher": {
                        "@id": "https://schema.datalad.org/publisher#Eisenberg",
                        "@type": "Organization",
                        "name": "Eisenberg"
                    },
                    "isPartOf": {
                        "@id": "#issue(500)",
                        "@type": "PublicationIssue",
                        "issue_number": 500
                    }
                },
                {
                    "@id": "#publication[3]",
                    "@type": "ScholarlyArticle",
                    "headline": "Publication Numero Quatro",
                    "datePublished": 2004,
                    "sameAs": "doi:1112-23232-3",
                    "author": [
                        {
                            "@id": "https://schema.datalad.org/person#a@example.com"
                        },
                        {
                            "@id": "https://schema.datalad.org/person#b@example.com"
                        }
                    ],
                    "publisher": {
                        "@id": "https://schema.datalad.org/publisher#Killian-Tumler",
                        "@type": "Organization",
                        "name": "Killian-Tumler"
                    }
                }
            ]
        }
    ]
}


def test_maximal_jsonld_creation():
    ldc = LDCreator(
        "id-0000--0000",
        "a02312398324778972389472834",
        ".studyminimeta.yaml")

    success, jsonld_object, _, messages = ldc.create_ld_from_spec(
        STUDYMINIMETA_METADATA_MAXIMAL_SPEC)

    assert success is True, messages
    assert jsonld_object == EXPECTED_MAXIMAL_JSON_LD


def test_minimal_jsonld_creation():
    ldc = LDCreator(
        "id-0000--0000",
        "a02312398324778972389472834",
        ".studyminimeta.yaml")

    minimal_spec = {
        "study": {
            "name": "Intelligence in Rodents",
            "start_date": "31.10.1990",
            "end_date": "22.12.2010",
            "keyword": ["Rodent"],
            "principal_investigator": "a@fz-juelich.de"
        },
        "dataset": {
            "name": "rdonernesdf-s.dfs",
            "location": "juseless:/data/project/riskystudy",
            "keyword": ["fMRI"],
            "author": ["a@fz-juelich.de"]
        },
        "person": {
            "a@fz-juelich.de": {
                "given_name": "Hans",
                "last_name": "Haber"
            }
        }
    }

    minimal_jsonld_template = {
        "@context": {
            "@vocab": "http://schema.org/"
        },
        "@graph": [
            {
                "@id": "#study",
                "@type": "CreativeWork",
                "name": "Intelligence in Rodents",
                "dateCreated": "31.10.1990",
                "keywords": [
                    "Rodent"
                ],
                "accountablePerson": "a@fz-juelich.de",
                "description": "end_date: 22.12.2010"
            },
            {
                "@id": "https://schema.datalad.org/datalad_dataset#id-0000--0000",
                "@type": "Dataset",
                "version": "a02312398324778972389472834",
                "name": "rdonernesdf-s.dfs",
                "url": "juseless:/data/project/riskystudy",
                "keywords": [
                    "fMRI"
                ],
                "author": [
                    {
                        "@id": "https://schema.datalad.org/person#a@fz-juelich.de"
                    }
                ],
            },
            {
                "@id": "#personList",
                "@list": [
                    {
                        "@id": "https://schema.datalad.org/person#a@fz-juelich.de",
                        "@type": "Person",
                        "email": "a@fz-juelich.de",
                        "name": " Hans Haber",
                        "givenName": "Hans",
                        "familyName": "Haber"
                    }
                ]
            }
        ]
    }

    success, jsonld_object, _, messages = ldc.create_ld_from_spec(
        minimal_spec)

    assert success is True, messages

    # Verify the description individually, since it is not in the
    # minimal JSON-LD template
    assert "description" in jsonld_object["@graph"][1]
    assert isinstance(jsonld_object["@graph"][1]["description"], str)
    del jsonld_object["@graph"][1]["description"]

    assert jsonld_object == minimal_jsonld_template


def test_error_key():
    ldc = LDCreator(
        "id-0000--0000",
        "a02312398324778972389472834",
        ".studyminimeta.yaml")

    success, _, _, messages = ldc.create_ld_from_spec({})
    assert success is False
    assert messages == ["'person'"]


def test_error_type():
    ldc = LDCreator(
        "id-0000--0000",
        "a02312398324778972389472834",
        ".studyminimeta.yaml")

    message_pattern = "'{}' object has no attribute 'items'"
    for faulty_spec in ([], "", None, 1):
        success, _, _, messages = ldc.create_ld_from_spec(faulty_spec)
        assert success is False
        assert messages == [message_pattern.format(type(faulty_spec).__name__)]


proper_yaml_1 = """
study:
  name: Intelligence in Rodents
  start_date: 31.10.1990
  end_date: 22.12.2010
  keyword:
    - Rodent
  principal_investigator: a@fz-juelich.de

dataset:
  name: Rodent-Intelligence Brainscans
  location: juseless:/data/project/riskystudy
  keyword:
    - fMRI
  author:
    - a@fz-juelich.de

person:
  a@fz-juelich.de:
    given_name: Hans
    last_name: Glück
"""


error_yaml_1 = """
study:
no way
this is correct: yaml
"""


error_yaml_2 = """
study:
  .    "

  ---- errord
"""


@mock.patch("datalad.distribution.dataset.Dataset")
@with_tree({".studyminimeta.yaml": proper_yaml_1})
def test_successful_yaml_parsing(dataset_mock=None, directory_path=None):

    dataset_mock.configure_mock(**{
        "pathobj": Path(directory_path),
        "config.obtain.return_value": ".studyminimeta.yaml"
    })
    results = tuple(StudyMiniMetaExtractor()(dataset_mock, "0000000", "all", "ok"))
    assert len(results) == 1
    assert results[0]["metadata"] != {}
    del results[0]["metadata"]
    assert results[0] == {
        "status": "ok",
        "type": "all"
    }


@mock.patch("datalad.distribution.dataset.Dataset")
@with_tree({"some_dir": {"somefile.yaml": proper_yaml_1}})
def test_config_honouring(dataset_mock=None, directory_path=None):

    dataset_mock.configure_mock(**{
        "pathobj": Path(directory_path),
        "config.obtain.return_value": "some_dir/somefile.yaml"
    })
    results = tuple(StudyMiniMetaExtractor()(dataset_mock, "0000000", "all", "ok"))
    assert len(results) == 1
    assert results[0]["metadata"] != {}
    del results[0]["metadata"]
    assert results[0] == {
        "status": "ok",
        "type": "all"
    }


@mock.patch("datalad.distribution.dataset.Dataset")
@with_tree({".studyminimeta.yaml": error_yaml_1})
def test_unsuccessful_yaml_parsing(dataset_mock=None, directory_path=None):

    dataset_mock.configure_mock(**{
        "pathobj": Path(directory_path),
        "config.obtain.return_value": ".studyminimeta.yaml"
    })

    results = tuple(StudyMiniMetaExtractor()(dataset_mock, "0000000", "all", "ok"))
    assert len(results) == 1
    assert results[0]["message"].startswith("YAML parsing failed with: ")
    del results[0]["message"]
    assert results[0] == {
        "status": "error",
        "metadata": {},
        "type": "all"
    }


@mock.patch("datalad.distribution.dataset.Dataset")
@with_tree({'.studyminimeta.yaml': error_yaml_2})
def test_schema_violation(dataset_mock=None, directory_path=None):

    dataset_mock.configure_mock(**{
        "pathobj": Path(directory_path),
        "config.obtain.return_value": ".studyminimeta.yaml"
    })

    results = tuple(StudyMiniMetaExtractor()(dataset_mock, "0000000", "all", "ok"))
    assert len(results) == 1
    assert results[0] == {
        "status": "error",
        "metadata": {},
        "type": "all",
        "message": "data structure conversion to JSON-LD failed"
    }


@mock.patch("datalad.distribution.dataset.Dataset")
@with_tree({"some_file.yaml": error_yaml_1})
def test_file_handling(dataset_mock=None, directory_path=None):

    dataset_mock.configure_mock(**{
        "pathobj": Path(directory_path),
        "config.obtain.return_value": ".studyminimeta.yaml"
    })
    results = tuple(StudyMiniMetaExtractor()(dataset_mock, "0000000", "all", "ok"))
    assert len(results) == 1
    assert results[0]["message"].startswith("file ")
    assert results[0]["message"].endswith(" could not be opened")
    del results[0]["message"]
    assert results[0] == {
        "status": "error",
        "metadata": {},
        "type": "all"
    }


def test_process_type():
    results = tuple(StudyMiniMetaExtractor()(None, None, "unsupported", None))
    assert results == tuple()


@with_tempfile(mkdir=True)
def test_package_availability(path=None):
    dataset = Dataset(path)
    dataset.create()
    (Path(path) / ".studyminimeta.yaml").write_text(proper_yaml_1)
    results = tuple(meta_extract(
        "metalad_studyminimeta",
        dataset=path
    ))
    assert results[0]["status"] == "ok"
    assert results[0]["metadata_record"]["extractor_name"] == "metalad_studyminimeta"
