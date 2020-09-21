# -*- coding: utf-8 -*-

from collections import namedtuple
from typing import Dict, List, Union


LDCreatorResult = namedtuple("LDCreatorResult", ["success", "json_ld_object", "keys", "messages"])


DATALAD_SCHEMA_BASE = "https://schema.datalad.org"

AUTHOR_KEY = "author"
CONTRIBUTOR_KEY = "contributor"
CORRESPONDING_AUTHOR_KEY = "corresponding_author"
DATASET_KEY = "dataset"
DESCRIPTION_KEY = "description"
FUNDING_KEY = "funding"
ISSUE_KEY = "issue"
LOCATION_KEY = "location"
NAME_KEY = "name"
PERSON_KEY = "person"
PUBLICATION_KEY = "publication"
STANDARD_KEY = "standard"
STUDY_KEY = "study"
VOLUME_KEY = "volume"


# Automatically translated study-element properties
STUDY_TRANSLATION_TABLE = {
    "name": "name",
    "principal_investigator": "accountablePerson",
    "keyword": "keywords",
    "purpose": "abstract",
    "start_date": "dateCreated",
}


# Automatically translated person-element properties
PERSON_TRANSLATION_TABLE = {
    "given_name": "givenName",
    "last_name": "familyName",
    "title": "honorificSuffix",
    "affiliation": "affiliation",
    "orcid-id": "sameAs"
}


# Automatically translated publication-element properties (to https://schema.org/ScholarlyArticle)
PUBLICATION_TRANSLATION_TABLE = {
    "title": "headline",
    "year": "datePublished",
    "doi": "sameAs",
    "pages": "pagination",
    CORRESPONDING_AUTHOR_KEY: "accountablePerson",
}


# Automatically translated dataset-element properties (to https://schema.org/Dataset)
DATASET_TRANSLATION_TABLE = {
    "name": "name",
    "location": "url",
    "keyword": "keywords",
    "description": "description",
}


class LDCreator(object):
    def __init__(self, dataset_id: str, ref_commit: str, metadata_file_name: str):
        self.dataset_id = dataset_id
        self.ref_commit = ref_commit
        self.base_id = self._get_dataset_jsonld_id(dataset_id) + f"/{metadata_file_name}"

    def _get_person_name(self, spec: dict):
        return f'{spec["title"] if "title" in spec else ""} {spec["given_name"]} {spec["last_name"]}'

    def _get_translated_dict(self, spec: Dict, translation_table: Dict[str, str]) -> Dict:
        return {
               translation_table[key]: value
               for key, value in spec.items() if key in translation_table
        }

    def _get_local_id_with_part(self, local_part: str) -> str:
        return f"#{local_part}"

    def _get_datalad_global_id(self, category: str, name: str):
        return f"{DATALAD_SCHEMA_BASE}/{category}#{name}"

    def _get_person_id(self, email: str) -> str:
        return self._get_datalad_global_id("person", email)

    def _get_organization_id(self, name: str) -> str:
        return self._get_datalad_global_id("organization", name)

    def _get_publisher_id(self, name: str) -> str:
        return self._get_datalad_global_id("publisher", name)

    def _get_dataset_jsonld_id(self, dataset_id: str) -> str:
        return self._get_datalad_global_id("datalad_dataset", dataset_id)

    def _get_issue_id(self, publication_index: int) -> str:
        return self._get_local_id_with_part(f"publication.{publication_index}.issue")

    def _get_volume_id(self, publication_index: int) -> str:
        return self._get_local_id_with_part(f"publication.{publication_index}.volume")

    def _get_volume_issue_dict(self, index: int, publication: dict) -> dict:
        volume = publication.get(VOLUME_KEY, None)
        issue = publication.get(ISSUE_KEY, None)
        if volume and issue:
            return {
                "@id": f"#issue({issue})",
                "@type": "PublicationIssue",
                "issueNumber": issue,
                "isPartOf": {
                    "@id": f"#volume({volume})",
                    "@type": "PublicationVolume",
                    "volumeNumber": volume,
                }
            }
        if issue:
            return {
                "@id": f"#issue({issue})",
                "@type": "PublicationIssue",
                "issueNumber": issue
        }
        if volume:
            return {
                "@id": f"#volume({volume})",
                "@type": "PublicationVolume",
                "volumeNumber": volume
            }
        return {}

    def _create_study_ld(self, study: dict) -> dict:
        return {
            "@id": "#study",
            "@type": "CreativeWork",
            **self._get_translated_dict(study, STUDY_TRANSLATION_TABLE),
            **{
                "description": f"end_date: {value}"
                for key, value in study.items() if key == "end_date"
            },
            **{
                "contributor": [
                    {
                        "@id": self._get_person_id(email),
                    } for email in value
                ]
                for key, value in study.items() if key == CONTRIBUTOR_KEY
            },
            **{
                "funder": [
                    {
                        "@id": self._get_organization_id(funder_name),
                        "@type": "Organization",
                        "name": funder_name
                    }
                    for funder_name in study[FUNDING_KEY]
                ]
                for key, value in study.items() if key == FUNDING_KEY
            }
        }

    def _create_dataset_ld(self, dataset: dict) -> dict:
        return {
            "@id": self._get_dataset_jsonld_id(self.dataset_id),
            "@type": "Dataset",
            "version": self.ref_commit,
            **self._get_translated_dict(dataset, DATASET_TRANSLATION_TABLE),
            **{
                "author": [
                    {
                        "@id": self._get_person_id(email),
                    } for email in value
                ]
                for key, value in dataset.items() if key == AUTHOR_KEY
            },
            **{
                "funder": [
                    {
                        "@id": self._get_organization_id(funder_name),
                        "@type": "Organization",
                        "name": funder_name
                    }
                    for funder_name in dataset[FUNDING_KEY]
                ]
                for key, value in dataset.items() if key == FUNDING_KEY
            },
            **{
                "hasPart": {
                    "@id": "#standards",
                    "@type": "DefinedTermSet",
                    "hasDefinedTerm": [
                        {
                            "@id": self._get_datalad_global_id("standard", standard),
                            "@type": "DefinedTerm",
                            "termCode": standard
                        }
                        for standard in dataset[STANDARD_KEY]
                    ]
                }
                for _ in [0] if STANDARD_KEY in dataset
            },
            **{
                "description": f"<this is an autogenerated description for dataset {self.dataset_id} ,"
                               "since no description was provided by the author, and "
                               "because google rich-results requires the description-property "
                               "in schmema.org/Dataset types>"
                for _ in [0] if DESCRIPTION_KEY not in dataset
            }
        }

    def _create_publication_list_ld(self, publication_list: list) -> list:
        return [
            {
                "@id": f"#publication[{publication_index}]",
                "@type": "ScholarlyArticle",
                **self._get_translated_dict(publication, PUBLICATION_TRANSLATION_TABLE),
                **{
                    "author": [
                        {
                            "@id": self._get_person_id(email)
                        } for email in value
                    ]
                    for key, value in publication.items() if key == AUTHOR_KEY
                },
                **{
                    "publisher": {
                        "@id": self._get_publisher_id(publisher_name),
                        "@type": "Organization",
                        "name": publisher_name
                    }
                    for key, publisher_name in publication.items() if key == "publisher"
                },
                **{
                    "publication": {
                        "@id": self._get_datalad_global_id("publication_event", publication_event_name),
                        "@type": "PublicationEvent",
                        "name": publication_event_name
                    }
                    for key, publication_event_name in publication.items() if key == "publication"
                },
                **{
                    "description": f"{CORRESPONDING_AUTHOR_KEY}: {value}"
                    for key, value in publication.items() if key == CORRESPONDING_AUTHOR_KEY
                },
                **{
                    "isPartOf": {
                        **self._get_volume_issue_dict(publication_index, publication)
                    }
                    for _ in [0] if VOLUME_KEY in publication or ISSUE_KEY in publication
                }
            } for publication_index, publication in enumerate(publication_list)
        ]

    def _create_person_list_ld(self, persons: dict) -> list:
        return [
            {
                "@id": self._get_person_id(email),
                "@type": "Person",
                "email": email,
                "name": self._get_person_name(details),
                **self._get_translated_dict(details, PERSON_TRANSLATION_TABLE),
                **{
                    "contactPoint": {
                        "@id": f"#contactPoint({email})",
                        "@type": "ContactPoint",
                        "description": value
                    }
                    for key, value in details.items() if key == "contact_information"
                },
            } for email, details in persons.items()
        ]

    def _create_ld_from_spec(self, spec: Dict[str, Union[Dict, List]]) -> LDCreatorResult:
        graph_elements = {
            key: {
                STUDY_KEY: self._create_study_ld,
                DATASET_KEY: self._create_dataset_ld,
                PERSON_KEY: self._create_person_list_ld,
                PUBLICATION_KEY: self._create_publication_list_ld
            }[key](sub_spec)
            for key, sub_spec in spec.items()
        }

        graph_elements["person"] = {
            "@id": "#personList",
            "@list": graph_elements["person"]
        }

        if "publication" in graph_elements:
            graph_elements["publication"] = {
                "@id": "#publicationList",
                "@list": graph_elements["publication"]
            }

        json_ld = {
            "@context": {
                "@vocab": "http://schema.org/",
            },
            "@graph": [value for _, value in graph_elements.items()]
        }
        return LDCreatorResult(True, json_ld, [value for _, value in graph_elements.items()], [])

    def create_ld_from_spec(self, spec: dict):
        try:
            return self._create_ld_from_spec(spec)
        except KeyError as key_error:
            return LDCreatorResult(False, None, [], [str(key_error)])
