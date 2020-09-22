# -*- coding: utf-8 -*-


STUDYMINIMETA_METADATA_SPEC = {
    "study": {
        "name": "An example study",
        "purpose": "a short description of the study",
        "principal_investigator": "a@example.com",
        "keyword": ["k1", "k2"],
        "funding": ["DFG", "NHI"],
        "contributor": ["a@example.com", "b@example.com"],
        "contact_information": "Corner broadway and main",
        "start_date": "01.01.2020",
        "end_date": "02.02.2020"
    },
    "dataset": {
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
    },
    "person": {
        "a@example.com": {
            "given_name": "Alex",
            "last_name": "Meyer",
            "orcid-id": "0000-0001-0002-0033",
            "title": "Prof. Dr.",
            "contact_information": "Corner broadway and main"
        },
        "b@example.com": {
            "given_name": "Bernd",
            "last_name": "Muller",
            "orcid-id": "0000-0001-0002-0034",
            "title": "MD",
            "contact_information": "central plaza"
        }
    },
    "publication": [
        {
            "title": "Publication Numero Unos",
            "year": 2001,
            "author": ["a@example.com", "b@example.com"],
            "doi": "doi:1112-23232-0",
            "volume": 30,
            "issue": 4,
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
}
