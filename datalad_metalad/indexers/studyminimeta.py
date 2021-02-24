import enum
from typing import Dict, Generator, List, Tuple, Union

from .jsonld import JsonLdProperties, JsonLdTags, JsonLdTypes
from datalad.metadata.indexers.base import MetadataIndexer


STUDYMINIMETA_FORMAT_NAME = 'metalad_studyminimeta'


class StudyMiniMetaTags(enum.Enum):
    pass


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
            if element.get(JsonLdTags.ID, None) == "#personList"
        ][0]

        study = [
            element
            for element in study_mini_meta[JsonLdTags.GRAPH]
            if element.get(JsonLdTags.ID, None) == "#study"
        ][0]

        dataset = [
            element
            for element in study_mini_meta[JsonLdTags.GRAPH]
            if element.get(JsonLdTags.TYPE, None) == JsonLdTypes.DATASET
        ][0]

        standards = [
            part[JsonLdProperties.TERM_CODE]
            for part in dataset.get(
                JsonLdProperties.HAS_PART,
                {JsonLdProperties.HAS_DEFINED_TERM: []}
            )[JsonLdProperties.HAS_DEFINED_TERM]
            if part[JsonLdTags.TYPE] == JsonLdTypes.DEFINED_TERM
        ]

        publications = ([
            element
            for element in study_mini_meta[JsonLdTags.GRAPH]
            if element.get(JsonLdTags.ID, None) == "#publicationList"
        ] or [None])[0]

        yield "dataset.author", ", ".join(
            [
                p[JsonLdProperties.NAME]
                for author_spec in dataset[JsonLdProperties.AUTHOR]
                for p in persons[JsonLdTags.LIST]
                if p[JsonLdTags.ID] == author_spec[JsonLdTags.ID]
            ]
        )

        yield "dataset.name", dataset[JsonLdProperties.NAME]
        yield "dataset.location", dataset[JsonLdProperties.URL]

        if JsonLdProperties.DESCRIPTION in dataset:
            yield "dataset.description", dataset[JsonLdProperties.DESCRIPTION]

        if JsonLdProperties.KEYWORDS in dataset:
            yield "dataset.keywords", ", ".join(dataset[JsonLdProperties.KEYWORDS])

        if JsonLdProperties.FUNDER in dataset:
            yield "dataset.funder", ", ".join(
                [
                    funder[JsonLdProperties.NAME]
                    for funder in dataset[JsonLdProperties.FUNDER]
                ]
            )

        if standards:
            yield "dataset.standards", ", ".join(standards)

        yield "person.email", ", ".join(
            [
                person[JsonLdProperties.EMAIL]
                for person in persons[JsonLdTags.LIST]
            ]
        )

        yield "person.name", ", ".join(
            [
                person[JsonLdProperties.NAME]
                for person in persons[JsonLdTags.LIST]
            ]
        )

        yield JsonLdProperties.NAME, study[JsonLdProperties.NAME]

        yield "accountable_person", [
            p[JsonLdProperties.NAME]
            for p in persons[JsonLdTags.LIST]
            if p[JsonLdProperties.EMAIL] == study[JsonLdProperties.ACCOUNTABLE_PERSON]
        ][0]

        if JsonLdProperties.CONTRIBUTOR in study:
            yield "contributor", ", ".join(
                [
                    p[JsonLdProperties.NAME]
                    for contributor_spec in study[JsonLdProperties.CONTRIBUTOR]
                    for p in persons[JsonLdTags.LIST]
                    if p[JsonLdTags.ID] == contributor_spec[JsonLdTags.ID]
                ]
            )

        if publications:
            for publication in publications[JsonLdTags.LIST]:
                # Yield authors
                yield "publication.author", ", ".join(
                    [
                        p[JsonLdProperties.NAME]
                        for author_spec in publication[JsonLdProperties.AUTHOR]
                        for p in persons[JsonLdTags.LIST]
                        if p[JsonLdTags.ID] == author_spec[JsonLdTags.ID]
                    ]
                )
                yield "publication.title", publication[JsonLdProperties.HEADLINE]
                yield "publication.year", publication[JsonLdProperties.DATE_PUBLISHED]

        if JsonLdProperties.KEYWORDS in study:
            yield "keywords", ", ".join(study.get(JsonLdProperties.KEYWORDS, []))

        if JsonLdProperties.FUNDER in study:
            yield "funder", ", ".join(
                [
                    funder[JsonLdProperties.NAME]
                    for funder in study[JsonLdProperties.FUNDER]
                ]
            )
