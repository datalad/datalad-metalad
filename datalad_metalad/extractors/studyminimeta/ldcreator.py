# -*- coding: utf-8 -*-

from collections import namedtuple
from typing import Dict, List, Union


LDCreatorResult = namedtuple("LDCreatorResult", ["success", "json_ld_object", "keys", "messages"])


DATALAD_SCHEMA_BASE = "https://schema.datalad.org"


class SMMProperties:
    AFFILIATION = "affiliation"
    AUTHOR = "author"
    CONTACT_INFORMATION = "contact_information"
    CONTRIBUTOR = "contributor"
    CORRESPONDING_AUTHOR = "corresponding_author"
    DATASET = "dataset"
    DESCRIPTION = "description"
    DOI = "doi"
    END_DATE = "end_date"
    FUNDING = "funding"
    GIVEN_NAME = "given_name"
    ISSUE = "issue"
    KEYWORD = "keyword"
    LAST_NAME = "last_name"
    LOCATION = "location"
    NAME = "name"
    ORCID_ID = "orcid-id"
    ORGANIZATION = "organization"
    PAGES = "pages"
    PERSON = "person"
    PRINCIPAL_INVESTIGATOR = "principal_investigator"
    PUBLICATION = "publication"
    PUBLISHER = "publisher"
    PURPOSE = "purpose"
    STANDARD = "standard"
    START_DATE = "start_date"
    STUDY = "study"
    TITLE = "title"
    VOLUME = "volume"
    YEAR = "year"


class DataladIdCategory:
    DATALAD_DATASET = "datalad_dataset"
    ORGANIZATION = "organization"
    PERSON = "person"
    PUBLISHER = "publisher"


class JsonLdTags:
    GRAPH = "@graph"
    ID = "@id"
    LIST = "@list"
    TYPE = "@type"


class JsonLdTypes:
    CONTACT_POINT = "ContactPoint"
    CREATIVE_WORK = "CreativeWork"
    DATASET = "Dataset"
    DEFINED_TERM = "DefinedTerm"
    DEFINED_TERM_SET = "DefinedTermSet"
    ORGANIZATION = "Organization"
    PERSON = "Person"
    PUBLICATION_EVENT = "PublicationEvent"
    PUBLICATION_ISSUE = "PublicationIssue"
    PUBLICATION_VOLUME = "PublicationVolume"
    SCHOLARLY_ARTICLE = "ScholarlyArticle"


class JsonLdProperties:
    ABSTRACT = "abstract"
    ACCOUNTABLE_PERSON = "accountablePerson"
    AFFILIATION = "affiliation"
    AUTHOR = "author"
    CONTACT_POINT = "contactPoint"
    CONTRIBUTOR = "contributor"
    DATASET = "dataset"
    DATE_CREATED = "dateCreated"
    DATE_PUBLISHED = "datePublished"
    DESCRIPTION = "description"
    EMAIL = "email"
    FAMILY_NAME = "familyName"
    FUNDER = "funder"
    GIVEN_NAME = "givenName"
    HAS_DEFINED_TERM = "hasDefinedTerm"
    HAS_PART = "hasPart"
    HEADLINE = "headline"
    HONORIFIC_SUFFIX = "honorificSuffix"
    IS_PART_OF = "isPartOf"
    ISSUE_NUMBER = "issue_number"
    KEYWORDS = "keywords"
    NAME = "name"
    PAGINATION = "pagination"
    PERSON = "person"
    PUBLICATION = "publication"
    PUBLISHER = "publisher"
    SAME_AS = "sameAs"
    STUDY = "study"
    TERM_CODE = "termCode"
    URL = "url"
    VERSION = "version"
    VOLUME_NUMBER = "volumeNumber"


# Automatically translated study-element properties
STUDY_TRANSLATION_TABLE = {
    SMMProperties.NAME: JsonLdProperties.NAME,
    SMMProperties.PRINCIPAL_INVESTIGATOR: JsonLdProperties.ACCOUNTABLE_PERSON,
    SMMProperties.KEYWORD: JsonLdProperties.KEYWORDS,
    SMMProperties.PURPOSE: JsonLdProperties.ABSTRACT,
    SMMProperties.START_DATE: JsonLdProperties.DATE_CREATED
}


# Automatically translated person-element properties
PERSON_TRANSLATION_TABLE = {
    SMMProperties.GIVEN_NAME: JsonLdProperties.GIVEN_NAME,
    SMMProperties.LAST_NAME: JsonLdProperties.FAMILY_NAME,
    SMMProperties.TITLE: JsonLdProperties.HONORIFIC_SUFFIX,
    SMMProperties.AFFILIATION: JsonLdProperties.AFFILIATION,
    SMMProperties.ORCID_ID: JsonLdProperties.SAME_AS
}


# Automatically translated publication-element properties (to https://schema.org/ScholarlyArticle)
PUBLICATION_TRANSLATION_TABLE = {
    SMMProperties.TITLE: JsonLdProperties.HEADLINE,
    SMMProperties.YEAR: JsonLdProperties.DATE_PUBLISHED,
    SMMProperties.DOI: JsonLdProperties.SAME_AS,
    SMMProperties.PAGES: JsonLdProperties.PAGINATION,
    SMMProperties.CORRESPONDING_AUTHOR: JsonLdProperties.ACCOUNTABLE_PERSON
}


# Automatically translated dataset-element properties (to https://schema.org/Dataset)
DATASET_TRANSLATION_TABLE = {
    SMMProperties.NAME: JsonLdProperties.NAME,
    SMMProperties.LOCATION: JsonLdProperties.URL,
    SMMProperties.KEYWORD: JsonLdProperties.KEYWORDS,
    SMMProperties.DESCRIPTION: JsonLdProperties.DESCRIPTION
}


class LDCreator(object):
    def __init__(self, dataset_id: str, ref_commit: str, metadata_file_name: str):
        self.dataset_id = dataset_id
        self.ref_commit = ref_commit
        self.base_id = self._get_dataset_jsonld_id(
            dataset_id) + "/{metadata_file_name}".format(
                metadata_file_name=metadata_file_name)

    def _get_person_name(self, spec: dict):
        return '{title} {given_name} {last_name}'.format(
            title=(
                spec[SMMProperties.TITLE] + " "
                if SMMProperties.TITLE in spec
                else ""),
            given_name=spec[SMMProperties.GIVEN_NAME],
            last_name=spec[SMMProperties.LAST_NAME])

    def _get_translated_dict(self, spec: Dict, translation_table: Dict[str, str]) -> Dict:
        return {
               translation_table[key]: value
               for key, value in spec.items() if key in translation_table
        }

    def _get_datalad_global_id(self, category: str, name: str):
        return "{datalad_schema_base}/{category}#{name}".format(
            datalad_schema_base=DATALAD_SCHEMA_BASE,
            category=category,
            name=name)

    def _get_person_id(self, email: str) -> str:
        return self._get_datalad_global_id(DataladIdCategory.PERSON, email)

    def _get_organization_id(self, name: str) -> str:
        return self._get_datalad_global_id(DataladIdCategory.ORGANIZATION, name)

    def _get_publisher_id(self, name: str) -> str:
        return self._get_datalad_global_id(DataladIdCategory.PUBLISHER, name)

    def _get_dataset_jsonld_id(self, dataset_id: str) -> str:
        return self._get_datalad_global_id(DataladIdCategory.DATALAD_DATASET, dataset_id)

    def _get_volume_issue_dict(self, index: int, publication: dict) -> dict:
        volume = publication.get(SMMProperties.VOLUME, None)
        issue = publication.get(SMMProperties.ISSUE, None)
        if volume and issue:
            return {
                JsonLdTags.ID: "#issue({issue})".format(issue=issue),
                JsonLdTags.TYPE: JsonLdTypes.PUBLICATION_ISSUE,
                JsonLdProperties.ISSUE_NUMBER: issue,
                JsonLdProperties.IS_PART_OF: {
                    JsonLdTags.ID: "#volume({volume})".format(volume=volume),
                    JsonLdTags.TYPE: JsonLdTypes.PUBLICATION_VOLUME,
                    JsonLdProperties.VOLUME_NUMBER: volume,
                }
            }
        if issue:
            return {
                JsonLdTags.ID: "#issue({issue})".format(issue=issue),
                JsonLdTags.TYPE: JsonLdTypes.PUBLICATION_ISSUE,
                JsonLdProperties.ISSUE_NUMBER: issue
            }
        if volume:
            return {
                JsonLdTags.ID: "#volume({volume})".format(volume=volume),
                JsonLdTags.TYPE: JsonLdTypes.PUBLICATION_VOLUME,
                JsonLdProperties.VOLUME_NUMBER: volume
            }
        return {}

    def _create_study_ld(self, study: dict) -> dict:
        return {
            JsonLdTags.ID: "#study",
            JsonLdTags.TYPE: JsonLdTypes.CREATIVE_WORK,
            **self._get_translated_dict(study, STUDY_TRANSLATION_TABLE),
            **{
                JsonLdProperties.DESCRIPTION: "end_date: {value}".format(value=value)
                for key, value in study.items() if key == SMMProperties.END_DATE
            },
            **{
                JsonLdProperties.CONTRIBUTOR: [
                    {
                        JsonLdTags.ID: self._get_person_id(email),
                    } for email in value
                ]
                for key, value in study.items() if key == SMMProperties.CONTRIBUTOR
            },
            **{
                JsonLdProperties.FUNDER: [
                    {
                        JsonLdTags.ID: self._get_organization_id(funder_name),
                        JsonLdTags.TYPE: JsonLdTypes.ORGANIZATION,
                        JsonLdProperties.NAME: funder_name
                    }
                    for funder_name in study[SMMProperties.FUNDING]
                ]
                for key, value in study.items() if key == SMMProperties.FUNDING
            }
        }

    def _create_dataset_ld(self, dataset: dict) -> dict:
        return {
            JsonLdTags.ID: self._get_dataset_jsonld_id(self.dataset_id),
            JsonLdTags.TYPE: JsonLdTypes.DATASET,
            JsonLdProperties.VERSION: self.ref_commit,
            **self._get_translated_dict(dataset, DATASET_TRANSLATION_TABLE),
            **{
                JsonLdProperties.AUTHOR: [
                    {
                        JsonLdTags.ID: self._get_person_id(email),
                    } for email in email_list
                ]
                for key, email_list in dataset.items()
                if key == SMMProperties.AUTHOR
            },
            **{
                JsonLdProperties.FUNDER: [
                    {
                        JsonLdTags.ID: self._get_organization_id(funder_name),
                        JsonLdTags.TYPE: JsonLdTypes.ORGANIZATION,
                        JsonLdProperties.NAME: funder_name
                    }
                    for funder_name in dataset[SMMProperties.FUNDING]
                ]
                for key, value in dataset.items()
                if key == SMMProperties.FUNDING
            },
            **{
                JsonLdProperties.HAS_PART: {
                    JsonLdTags.ID: "#standards",
                    JsonLdTags.TYPE: JsonLdTypes.DEFINED_TERM_SET,
                    JsonLdProperties.HAS_DEFINED_TERM: [
                        {
                            JsonLdTags.ID: self._get_datalad_global_id("standard", standard),
                            JsonLdTags.TYPE: JsonLdTypes.DEFINED_TERM,
                            JsonLdProperties.TERM_CODE: standard
                        }
                        for standard in dataset[SMMProperties.STANDARD]
                    ]
                }
                for _ in [0]
                if SMMProperties.STANDARD in dataset
            },
            **{
                JsonLdProperties.DESCRIPTION:
                    "<this is an autogenerated description for dataset {dataset_id} ,"
                    "since no description was provided by the author, and "
                    "because google rich-results requires the description-property "
                    "in schmema.org/Dataset types>".format(dataset_id=self.dataset_id)
                for _ in [0]
                if SMMProperties.DESCRIPTION not in dataset
            }
        }

    def _create_publication_list_ld(self, publication_list: list) -> list:
        return [
            {
                JsonLdTags.ID: "#publication[{publication_index}]".format(publication_index=publication_index),
                JsonLdTags.TYPE: JsonLdTypes.SCHOLARLY_ARTICLE,
                **self._get_translated_dict(publication, PUBLICATION_TRANSLATION_TABLE),
                **{
                    JsonLdProperties.AUTHOR: [
                        {
                            JsonLdTags.ID: self._get_person_id(email)
                        } for email in email_list
                    ]
                    for key, email_list in publication.items()
                    if key == SMMProperties.AUTHOR
                },
                **{
                    JsonLdProperties.PUBLISHER: {
                        JsonLdTags.ID: self._get_publisher_id(publisher_name),
                        JsonLdTags.TYPE: JsonLdTypes.ORGANIZATION,
                        JsonLdProperties.NAME: publisher_name
                    }
                    for key, publisher_name in publication.items()
                    if key == SMMProperties.PUBLISHER
                },
                **{
                    JsonLdProperties.PUBLICATION: {
                        JsonLdTags.ID: self._get_datalad_global_id("publication_event", publication_event_name),
                        JsonLdTags.TYPE: JsonLdTypes.PUBLICATION_EVENT,
                        JsonLdProperties.NAME: publication_event_name
                    }
                    for key, publication_event_name in publication.items()
                    if key == SMMProperties.PUBLICATION
                },
                **{
                    JsonLdProperties.DESCRIPTION: "{corresponding_author}: {publication}".format(
                        corresponding_author=SMMProperties.CORRESPONDING_AUTHOR,
                        publication=publication[SMMProperties.CORRESPONDING_AUTHOR]
                    )
                    for _ in [0]
                    if SMMProperties.CORRESPONDING_AUTHOR in publication
                },
                **{
                    JsonLdProperties.IS_PART_OF: {
                        **self._get_volume_issue_dict(publication_index, publication)
                    }
                    for _ in [0]
                    if SMMProperties.VOLUME in publication or SMMProperties.ISSUE in publication
                }
            } for publication_index, publication in enumerate(publication_list)
        ]

    def _create_person_list_ld(self, persons: dict) -> list:
        return [
            {
                JsonLdTags.ID: self._get_person_id(email),
                JsonLdTags.TYPE: JsonLdTypes.PERSON,
                JsonLdProperties.EMAIL: email,
                JsonLdProperties.NAME: self._get_person_name(details),
                **self._get_translated_dict(details, PERSON_TRANSLATION_TABLE),
                **{
                    JsonLdProperties.CONTACT_POINT: {
                        JsonLdTags.ID: "#contactPoint({email})".format(email=email),
                        JsonLdTags.TYPE: JsonLdTypes.CONTACT_POINT,
                        JsonLdProperties.DESCRIPTION: details[SMMProperties.CONTACT_INFORMATION]
                    }
                    for _ in [0]
                    if SMMProperties.CONTACT_INFORMATION in details
                },
            } for email, details in persons.items()
        ]

    def _create_ld_from_spec(self, spec: Dict[str, Union[Dict, List]]) -> LDCreatorResult:
        graph_elements = {
            key: {
                SMMProperties.STUDY: self._create_study_ld,
                SMMProperties.DATASET: self._create_dataset_ld,
                SMMProperties.PERSON: self._create_person_list_ld,
                SMMProperties.PUBLICATION: self._create_publication_list_ld
            }[key](sub_spec)
            for key, sub_spec in spec.items()
        }

        graph_elements[JsonLdProperties.PERSON] = {
            JsonLdTags.ID: "#personList",
            JsonLdTags.LIST: graph_elements[JsonLdProperties.PERSON]
        }

        if JsonLdProperties.PUBLICATION in graph_elements:
            graph_elements[JsonLdProperties.PUBLICATION] = {
                JsonLdTags.ID: "#publicationList",
                JsonLdTags.LIST: graph_elements[JsonLdProperties.PUBLICATION]
            }

        json_ld = {
            "@context": {
                "@vocab": "http://schema.org/",
            },
            JsonLdTags.GRAPH: [value for _, value in graph_elements.items()]
        }
        return LDCreatorResult(True, json_ld, [value for _, value in graph_elements.items()], [])

    def create_ld_from_spec(self, spec: dict) -> LDCreatorResult:
        try:
            return self._create_ld_from_spec(spec)
        except (KeyError, AttributeError) as exception:
            return LDCreatorResult(False, None, [], [str(exception)])
