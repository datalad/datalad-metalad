from typing import (
    Dict,
    Generator,
    List,
    Tuple,
    Union,
)

from datalad.metadata.indexers.base import MetadataIndexer

from .jsonld import (
    IndexerSchemaOrgProperties,
    IndexerJsonLdTags,
    IndexerSchemaOrgTypes,
)


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
    metadata_format_name : str
      name of metadata format the should be extracted

    Returns
    -------
    dict:
       key-value pairs representing the information in metadata.
       values can be literals or lists of literals
    """

    def __init__(self, metadata_format_name: str):
        super(StudyMiniMetaIndexer, self).__init__(metadata_format_name)
        if metadata_format_name != STUDYMINIMETA_FORMAT_NAME:
            raise ValueError(
                f"Indexer: {type(self).__name__} does not support "
                f"metadata format: {metadata_format_name}"
            )

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
            for element in study_mini_meta[IndexerJsonLdTags.GRAPH]
            if element.get(IndexerJsonLdTags.ID, None) == StudyMiniMetaIds.PERSON_LIST
        ][0]

        study = [
            element
            for element in study_mini_meta[IndexerJsonLdTags.GRAPH]
            if element.get(IndexerJsonLdTags.ID, None) == StudyMiniMetaIds.STUDY
        ][0]

        dataset = [
            element
            for element in study_mini_meta[IndexerJsonLdTags.GRAPH]
            if element.get(IndexerJsonLdTags.TYPE, None) == IndexerSchemaOrgTypes.DATASET
        ][0]

        standards = [
            part[IndexerSchemaOrgProperties.TERM_CODE]
            for part in dataset.get(
                IndexerSchemaOrgProperties.HAS_PART,
                {IndexerSchemaOrgProperties.HAS_DEFINED_TERM: []}
            )[IndexerSchemaOrgProperties.HAS_DEFINED_TERM]
            if part[IndexerJsonLdTags.TYPE] == IndexerSchemaOrgTypes.DEFINED_TERM
        ]

        publications = ([
            element
            for element in study_mini_meta[IndexerJsonLdTags.GRAPH]
            if element.get(IndexerJsonLdTags.ID, None) == StudyMiniMetaIds.PUBLICATION_LIST
        ] or [None])[0]

        yield "dataset.author", ", ".join(
            [
                p[IndexerSchemaOrgProperties.NAME]
                for author_spec in dataset[IndexerSchemaOrgProperties.AUTHOR]
                for p in persons[IndexerJsonLdTags.LIST]
                if p[IndexerJsonLdTags.ID] == author_spec[IndexerJsonLdTags.ID]
            ]
        )

        yield "dataset.name", dataset[IndexerSchemaOrgProperties.NAME]
        yield "dataset.location", dataset[IndexerSchemaOrgProperties.URL]

        if IndexerSchemaOrgProperties.DESCRIPTION in dataset:
            yield "dataset.description", dataset[IndexerSchemaOrgProperties.DESCRIPTION]

        if IndexerSchemaOrgProperties.KEYWORDS in dataset:
            yield "dataset.keywords", ", ".join(dataset[IndexerSchemaOrgProperties.KEYWORDS])

        if IndexerSchemaOrgProperties.FUNDER in dataset:
            yield "dataset.funder", ", ".join(
                [
                    funder[IndexerSchemaOrgProperties.NAME]
                    for funder in dataset[IndexerSchemaOrgProperties.FUNDER]
                ]
            )

        if standards:
            yield "dataset.standards", ", ".join(standards)

        yield "person.email", ", ".join(
            [
                person[IndexerSchemaOrgProperties.EMAIL]
                for person in persons[IndexerJsonLdTags.LIST]
            ]
        )

        yield "person.name", ", ".join(
            [
                person[IndexerSchemaOrgProperties.NAME]
                for person in persons[IndexerJsonLdTags.LIST]
            ]
        )

        yield IndexerSchemaOrgProperties.NAME, study[IndexerSchemaOrgProperties.NAME]

        yield "accountable_person", [
            p[IndexerSchemaOrgProperties.NAME]
            for p in persons[IndexerJsonLdTags.LIST]
            if p[IndexerSchemaOrgProperties.EMAIL] == study[IndexerSchemaOrgProperties.ACCOUNTABLE_PERSON]
        ][0]

        if IndexerSchemaOrgProperties.CONTRIBUTOR in study:
            yield "contributor", ", ".join(
                [
                    p[IndexerSchemaOrgProperties.NAME]
                    for contributor_spec in study[IndexerSchemaOrgProperties.CONTRIBUTOR]
                    for p in persons[IndexerJsonLdTags.LIST]
                    if p[IndexerJsonLdTags.ID] == contributor_spec[IndexerJsonLdTags.ID]
                ]
            )

        if publications:
            for publication in publications[IndexerJsonLdTags.LIST]:
                yield "publication.author", ", ".join(
                    [
                        p[IndexerSchemaOrgProperties.NAME]
                        for author_spec in publication[IndexerSchemaOrgProperties.AUTHOR]
                        for p in persons[IndexerJsonLdTags.LIST]
                        if p[IndexerJsonLdTags.ID] == author_spec[IndexerJsonLdTags.ID]
                    ]
                )
                yield "publication.title", publication[IndexerSchemaOrgProperties.HEADLINE]
                yield "publication.year", publication[IndexerSchemaOrgProperties.DATE_PUBLISHED]

        if IndexerSchemaOrgProperties.KEYWORDS in study:
            yield "keywords", ", ".join(study.get(IndexerSchemaOrgProperties.KEYWORDS, []))

        if IndexerSchemaOrgProperties.FUNDER in study:
            yield "funder", ", ".join(
                [
                    funder[IndexerSchemaOrgProperties.NAME]
                    for funder in study[IndexerSchemaOrgProperties.FUNDER]
                ]
            )
