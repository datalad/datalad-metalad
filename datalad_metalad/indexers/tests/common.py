MOCKED_STUDYMINIMETA_JSONLD = {
    "@context": {
        "@vocab": "http://schema.org/"
    },
    "@graph": [
        {
            "@id": "#study",
            "@type": "CreativeWork",
            "name": "A small study",
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
                    "@id": "https://schema.datalad.org/organization#NHO",
                    "@type": "Organization",
                    "name": "NHO"
                }
            ]
        },
        {
            "@id": "https://schema.datalad.org/datalad_dataset#id-0000--0000",
            "@type": "Dataset",
            "version": "a02312398324778972389472834",
            "name": "Datenddaslk",
            "url": "http://dlksdfs.comom.com",
            "description": "Some data I collected once upon a time, spending hours and hours in dark places.",
            "keywords": [
                "d_k1",
                "d_k2",
                "d_k3"
            ],
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
                    "name": "ds-funder-1"
                },
                {
                    "@id": "https://schema.datalad.org/organization#NHO",
                    "@type": "Organization",
                    "name": "ds-funder-2"
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
                        "issueNumber": 4,
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
                        "issueNumber": 500
                    }
                },
                {
                    "@id": "#publication[3]",
                    "@type": "ScholarlyArticle",
                    "headline": "Publication Numero Quatro",
                    "datePublished": 2004,
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
