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


class SMMJsonLdTags:
    CONTEXT = "@context"
    GRAPH = "@graph"
    ID = "@id"
    LIST = "@list"
    TYPE = "@type"
    VOCAB = "@vocab"


class SMMSchemaOrgTypes:
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


class SMMSchemaOrgProperties:
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
    SMMProperties.NAME: SMMSchemaOrgProperties.NAME,
    SMMProperties.PRINCIPAL_INVESTIGATOR: SMMSchemaOrgProperties.ACCOUNTABLE_PERSON,
    SMMProperties.KEYWORD: SMMSchemaOrgProperties.KEYWORDS,
    SMMProperties.PURPOSE: SMMSchemaOrgProperties.ABSTRACT,
    SMMProperties.START_DATE: SMMSchemaOrgProperties.DATE_CREATED
}


# Automatically translated person-element properties
PERSON_TRANSLATION_TABLE = {
    SMMProperties.GIVEN_NAME: SMMSchemaOrgProperties.GIVEN_NAME,
    SMMProperties.LAST_NAME: SMMSchemaOrgProperties.FAMILY_NAME,
    SMMProperties.TITLE: SMMSchemaOrgProperties.HONORIFIC_SUFFIX,
    SMMProperties.AFFILIATION: SMMSchemaOrgProperties.AFFILIATION,
    SMMProperties.ORCID_ID: SMMSchemaOrgProperties.SAME_AS
}


# Automatically translated publication-element properties (to https://schema.org/ScholarlyArticle)
PUBLICATION_TRANSLATION_TABLE = {
    SMMProperties.TITLE: SMMSchemaOrgProperties.HEADLINE,
    SMMProperties.YEAR: SMMSchemaOrgProperties.DATE_PUBLISHED,
    SMMProperties.DOI: SMMSchemaOrgProperties.SAME_AS,
    SMMProperties.PAGES: SMMSchemaOrgProperties.PAGINATION,
    SMMProperties.CORRESPONDING_AUTHOR: SMMSchemaOrgProperties.ACCOUNTABLE_PERSON
}


# Automatically translated dataset-element properties (to https://schema.org/Dataset)
DATASET_TRANSLATION_TABLE = {
    SMMProperties.NAME: SMMSchemaOrgProperties.NAME,
    SMMProperties.LOCATION: SMMSchemaOrgProperties.URL,
    SMMProperties.KEYWORD: SMMSchemaOrgProperties.KEYWORDS,
    SMMProperties.DESCRIPTION: SMMSchemaOrgProperties.DESCRIPTION
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
                SMMJsonLdTags.ID: "#issue({issue})".format(issue=issue),
                SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.PUBLICATION_ISSUE,
                SMMSchemaOrgProperties.ISSUE_NUMBER: issue,
                SMMSchemaOrgProperties.IS_PART_OF: {
                    SMMJsonLdTags.ID: "#volume({volume})".format(volume=volume),
                    SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.PUBLICATION_VOLUME,
                    SMMSchemaOrgProperties.VOLUME_NUMBER: volume,
                }
            }
        if issue:
            return {
                SMMJsonLdTags.ID: "#issue({issue})".format(issue=issue),
                SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.PUBLICATION_ISSUE,
                SMMSchemaOrgProperties.ISSUE_NUMBER: issue
            }
        if volume:
            return {
                SMMJsonLdTags.ID: "#volume({volume})".format(volume=volume),
                SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.PUBLICATION_VOLUME,
                SMMSchemaOrgProperties.VOLUME_NUMBER: volume
            }
        return {}

    def _create_study_ld(self, study: dict) -> dict:
        return {
            SMMJsonLdTags.ID: "#study",
            SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.CREATIVE_WORK,
            **self._get_translated_dict(study, STUDY_TRANSLATION_TABLE),
            **{
                SMMSchemaOrgProperties.DESCRIPTION: "end_date: {value}".format(value=value)
                for key, value in study.items() if key == SMMProperties.END_DATE
            },
            **{
                SMMSchemaOrgProperties.CONTRIBUTOR: [
                    {
                        SMMJsonLdTags.ID: self._get_person_id(email),
                    } for email in value
                ]
                for key, value in study.items() if key == SMMProperties.CONTRIBUTOR
            },
            **{
                SMMSchemaOrgProperties.FUNDER: [
                    {
                        SMMJsonLdTags.ID: self._get_organization_id(funder_name),
                        SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.ORGANIZATION,
                        SMMSchemaOrgProperties.NAME: funder_name
                    }
                    for funder_name in study[SMMProperties.FUNDING]
                ]
                for key, value in study.items() if key == SMMProperties.FUNDING
            }
        }

    def _create_dataset_ld(self, dataset: dict) -> dict:
        return {
            SMMJsonLdTags.ID: self._get_dataset_jsonld_id(self.dataset_id),
            SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.DATASET,
            SMMSchemaOrgProperties.VERSION: self.ref_commit,
            **self._get_translated_dict(dataset, DATASET_TRANSLATION_TABLE),
            **{
                SMMSchemaOrgProperties.AUTHOR: [
                    {
                        SMMJsonLdTags.ID: self._get_person_id(email),
                    } for email in email_list
                ]
                for key, email_list in dataset.items()
                if key == SMMProperties.AUTHOR
            },
            **{
                SMMSchemaOrgProperties.FUNDER: [
                    {
                        SMMJsonLdTags.ID: self._get_organization_id(funder_name),
                        SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.ORGANIZATION,
                        SMMSchemaOrgProperties.NAME: funder_name
                    }
                    for funder_name in dataset[SMMProperties.FUNDING]
                ]
                for key, value in dataset.items()
                if key == SMMProperties.FUNDING
            },
            **{
                SMMSchemaOrgProperties.HAS_PART: {
                    SMMJsonLdTags.ID: "#standards",
                    SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.DEFINED_TERM_SET,
                    SMMSchemaOrgProperties.HAS_DEFINED_TERM: [
                        {
                            SMMJsonLdTags.ID: self._get_datalad_global_id("standard", standard),
                            SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.DEFINED_TERM,
                            SMMSchemaOrgProperties.TERM_CODE: standard
                        }
                        for standard in dataset[SMMProperties.STANDARD]
                    ]
                }
                for _ in [0]
                if SMMProperties.STANDARD in dataset
            },
            **{
                SMMSchemaOrgProperties.DESCRIPTION:
                    "<this is an autogenerated description for dataset {dataset_id}, "
                    "since no description was provided by the author, and "
                    "because google rich-results requires the description-property "
                    "in schmema.org/Dataset metadatatypes>".format(dataset_id=self.dataset_id)
                for _ in [0]
                if SMMProperties.DESCRIPTION not in dataset
            }
        }

    def _create_publication_list_ld(self, publication_list: list) -> list:
        return [
            {
                SMMJsonLdTags.ID: "#publication[{publication_index}]".format(publication_index=publication_index),
                SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.SCHOLARLY_ARTICLE,
                **self._get_translated_dict(publication, PUBLICATION_TRANSLATION_TABLE),
                **{
                    SMMSchemaOrgProperties.AUTHOR: [
                        {
                            SMMJsonLdTags.ID: self._get_person_id(email)
                        } for email in email_list
                    ]
                    for key, email_list in publication.items()
                    if key == SMMProperties.AUTHOR
                },
                **{
                    SMMSchemaOrgProperties.PUBLISHER: {
                        SMMJsonLdTags.ID: self._get_publisher_id(publisher_name),
                        SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.ORGANIZATION,
                        SMMSchemaOrgProperties.NAME: publisher_name
                    }
                    for key, publisher_name in publication.items()
                    if key == SMMProperties.PUBLISHER
                },
                **{
                    SMMSchemaOrgProperties.PUBLICATION: {
                        SMMJsonLdTags.ID: self._get_datalad_global_id("publication_event", publication_event_name),
                        SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.PUBLICATION_EVENT,
                        SMMSchemaOrgProperties.NAME: publication_event_name
                    }
                    for key, publication_event_name in publication.items()
                    if key == SMMProperties.PUBLICATION
                },
                **{
                    SMMSchemaOrgProperties.DESCRIPTION: "{corresponding_author}: {publication}".format(
                        corresponding_author=SMMProperties.CORRESPONDING_AUTHOR,
                        publication=publication[SMMProperties.CORRESPONDING_AUTHOR]
                    )
                    for _ in [0]
                    if SMMProperties.CORRESPONDING_AUTHOR in publication
                },
                **{
                    SMMSchemaOrgProperties.IS_PART_OF: {
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
                SMMJsonLdTags.ID: self._get_person_id(email),
                SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.PERSON,
                SMMSchemaOrgProperties.EMAIL: email,
                SMMSchemaOrgProperties.NAME: self._get_person_name(details),
                **self._get_translated_dict(details, PERSON_TRANSLATION_TABLE),
                **{
                    SMMSchemaOrgProperties.CONTACT_POINT: {
                        SMMJsonLdTags.ID: "#contactPoint({email})".format(email=email),
                        SMMJsonLdTags.TYPE: SMMSchemaOrgTypes.CONTACT_POINT,
                        SMMSchemaOrgProperties.DESCRIPTION: details[SMMProperties.CONTACT_INFORMATION]
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

        graph_elements[SMMSchemaOrgProperties.PERSON] = {
            SMMJsonLdTags.ID: "#personList",
            SMMJsonLdTags.LIST: graph_elements[SMMSchemaOrgProperties.PERSON]
        }

        if SMMSchemaOrgProperties.PUBLICATION in graph_elements:
            graph_elements[SMMSchemaOrgProperties.PUBLICATION] = {
                SMMJsonLdTags.ID: "#publicationList",
                SMMJsonLdTags.LIST: graph_elements[SMMSchemaOrgProperties.PUBLICATION]
            }

        json_ld = {
            SMMJsonLdTags.CONTEXT: {
                SMMJsonLdTags.VOCAB: "http://schema.org/",
            },
            SMMJsonLdTags.GRAPH: [value for _, value in graph_elements.items()]
        }
        return LDCreatorResult(True, json_ld, [value for _, value in graph_elements.items()], [])

    def create_ld_from_spec(self, spec: dict) -> LDCreatorResult:
        try:
            return self._create_ld_from_spec(spec)
        except (KeyError, AttributeError) as exception:
            return LDCreatorResult(False, None, [], [str(exception)])
