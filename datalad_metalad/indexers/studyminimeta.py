from typing import Dict, Generator, List, Tuple, Union

from .jsonld import SchemaOrgProperties, JsonLdTags, SchemaOrgTypes
from datalad.metadata.indexers.base import MetadataIndexer


STUDYMINIMETA_FORMAT_NAME = 'metalad_studyminimeta'


class StudyMiniMetaIds:
    PERSON_LIST = "#personList"
    PUBLICATION_LIST = "#publicationList"
    STUDY = "#study"


class StudyMiniMetaIndexer(MetadataIndexer):
    """
    Indexer for metadata that was extracted from studyminimeta
    metadata (usually contained in ".studyminimeta.yaml"-files).

    Parameters
    ----------
    metadata : Dict or List
      Metadata created by an extractor.

    Returns
    -------
    dict:
       key-value pairs representing the information in metadata.
       values can be literals or lists of literals
    """

    def __init__(self, metadata_format_name: str):
        super(StudyMiniMetaIndexer, self).__init__(metadata_format_name)
        assert metadata_format_name == STUDYMINIMETA_FORMAT_NAME

    def create_index(self,
                     study_mini_meta: Union[Dict, List]
                     ) -> Generator[Tuple[str, str], None, None]:

        """
        Create the index from study-minimeta metadata.

        Parameters
        ----------
        study_mini_meta : JSON-LD structure the represents
          study-mini-meta metadata, which was created by a
          study-mini-meta extractor.

        Returns
        -------
        dict:
           key-value pairs representing the information in
           study_mini_meta as key-value pairs. Values are
           either literals or lists of literals.
        """
        persons = [
            element
            for element in study_mini_meta[JsonLdTags.GRAPH]
            if element.get(JsonLdTags.ID, None) == StudyMiniMetaIds.PERSON_LIST
        ][0]

        study = [
            element
            for element in study_mini_meta[JsonLdTags.GRAPH]
            if element.get(JsonLdTags.ID, None) == StudyMiniMetaIds.STUDY
        ][0]

        dataset = [
            element
            for element in study_mini_meta[JsonLdTags.GRAPH]
            if element.get(JsonLdTags.TYPE, None) == SchemaOrgTypes.DATASET
        ][0]

        standards = [
            part[SchemaOrgProperties.TERM_CODE]
            for part in dataset.get(
                SchemaOrgProperties.HAS_PART,
                {SchemaOrgProperties.HAS_DEFINED_TERM: []}
            )[SchemaOrgProperties.HAS_DEFINED_TERM]
            if part[JsonLdTags.TYPE] == SchemaOrgTypes.DEFINED_TERM
        ]

        publications = ([
            element
            for element in study_mini_meta[JsonLdTags.GRAPH]
            if element.get(JsonLdTags.ID, None) == StudyMiniMetaIds.PUBLICATION_LIST
        ] or [None])[0]

        yield "dataset.author", ", ".join(
            [
                p[SchemaOrgProperties.NAME]
                for author_spec in dataset[SchemaOrgProperties.AUTHOR]
                for p in persons[JsonLdTags.LIST]
                if p[JsonLdTags.ID] == author_spec[JsonLdTags.ID]
            ]
        )

        yield "dataset.name", dataset[SchemaOrgProperties.NAME]
        yield "dataset.location", dataset[SchemaOrgProperties.URL]

        if SchemaOrgProperties.DESCRIPTION in dataset:
            yield "dataset.description", dataset[SchemaOrgProperties.DESCRIPTION]

        if SchemaOrgProperties.KEYWORDS in dataset:
            yield "dataset.keywords", ", ".join(dataset[SchemaOrgProperties.KEYWORDS])

        if SchemaOrgProperties.FUNDER in dataset:
            yield "dataset.funder", ", ".join(
                [
                    funder[SchemaOrgProperties.NAME]
                    for funder in dataset[SchemaOrgProperties.FUNDER]
                ]
            )

        if standards:
            yield "dataset.standards", ", ".join(standards)

        yield "person.email", ", ".join(
            [
                person[SchemaOrgProperties.EMAIL]
                for person in persons[JsonLdTags.LIST]
            ]
        )

        yield "person.name", ", ".join(
            [
                person[SchemaOrgProperties.NAME]
                for person in persons[JsonLdTags.LIST]
            ]
        )

        yield SchemaOrgProperties.NAME, study[SchemaOrgProperties.NAME]

        yield "accountable_person", [
            p[SchemaOrgProperties.NAME]
            for p in persons[JsonLdTags.LIST]
            if p[SchemaOrgProperties.EMAIL] == study[SchemaOrgProperties.ACCOUNTABLE_PERSON]
        ][0]

        if SchemaOrgProperties.CONTRIBUTOR in study:
            yield "contributor", ", ".join(
                [
                    p[SchemaOrgProperties.NAME]
                    for contributor_spec in study[SchemaOrgProperties.CONTRIBUTOR]
                    for p in persons[JsonLdTags.LIST]
                    if p[JsonLdTags.ID] == contributor_spec[JsonLdTags.ID]
                ]
            )

        if publications:
            for publication in publications[JsonLdTags.LIST]:
                yield "publication.author", ", ".join(
                    [
                        p[SchemaOrgProperties.NAME]
                        for author_spec in publication[SchemaOrgProperties.AUTHOR]
                        for p in persons[JsonLdTags.LIST]
                        if p[JsonLdTags.ID] == author_spec[JsonLdTags.ID]
                    ]
                )
                yield "publication.title", publication[SchemaOrgProperties.HEADLINE]
                yield "publication.year", publication[SchemaOrgProperties.DATE_PUBLISHED]

        if SchemaOrgProperties.KEYWORDS in study:
            yield "keywords", ", ".join(study.get(SchemaOrgProperties.KEYWORDS, []))

        if SchemaOrgProperties.FUNDER in study:
            yield "funder", ", ".join(
                [
                    funder[SchemaOrgProperties.NAME]
                    for funder in study[SchemaOrgProperties.FUNDER]
                ]
            )
