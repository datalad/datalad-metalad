# -*- coding: utf-8 -*-

from datalad_metalad.extractors.studyminimeta.ld_creator import LDCreator


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


STUDYMINIMETA_METADATA_SPEC = {
    "study": STUDYMINIMETA_STUDY,
    "dataset": STUDYMINIMETA_DATASET,
    "person": STUDYMINIMETA_PERSONS,
    "publication": STUDYMINIMETA_PUBLICATIONS
}


EXPECTED_JSON_LD = {
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


def test_jsonld_creator():
    ldc = LDCreator(
        "id-0000--0000",
        "a02312398324778972389472834",
        ".studyminimeta.yaml")

    success, jsonld_object, keys, messages = ldc.create_ld_from_spec(
        STUDYMINIMETA_METADATA_SPEC)

    assert success is True, messages
    assert jsonld_object == EXPECTED_JSON_LD
