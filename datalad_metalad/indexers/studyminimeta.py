from typing import Dict, List, Union

from .base import MetadataIndexer


class StudyMiniMetaIndexer(MetadataIndexer):
    def __call__(self, metadata_format_name: str, study_mini_meta: Union[Dict, List]) -> Dict:

        # This is the only format, this indexer currently understands
        assert metadata_format_name == "metalad_studyminimeta"

        persons = [element for element in study_mini_meta["@graph"] if element.get("@id", None) == "#personList"][0]
        study = [element for element in study_mini_meta["@graph"] if element.get("@id", None) == "#study"][0]
        dataset = [element for element in study_mini_meta["@graph"] if element.get("@type", None) == "Dataset"][0]
        standards = ([
            part["termCode"]
            for part in dataset.get("hasPart", {"hasDefinedTerm": []})["hasDefinedTerm"]
            if part["@type"] == "DefinedTerm"
        ])
        publications = (
                [element for element in study_mini_meta["@graph"]
                 if element.get("@id", None) == "#publicationList"] or [None]
        )[0]

        yield (
            "dataset.author",
            ", ".join([
                p["name"]
                for author_spec in dataset["author"]
                for p in persons["@list"] if p["@id"] == author_spec["@id"]
            ])
        )
        yield ("dataset.name", dataset["name"])
        yield ("dataset.location", dataset["url"])

        if "description" in dataset:
            yield ("dataset.description", dataset["description"])

        if "funder" in dataset:
            yield (
                "dataset.funder",
                ", ".join([funder["name"] for funder in dataset["funder"]])
            )

        if "keywords" in dataset:
            yield ("dataset.keywords", ", ".join(dataset["keywords"]))

        if standards:
            yield ("dataset.standards", ", ".join(standards))

        yield ("person.email", ", ".join([person["email"] for person in persons["@list"]]))
        yield ("person.name", ", ".join([person["name"] for person in persons["@list"]]))

        yield ("name", study["name"])

        yield (
            "accountable_person",
            [
                p["name"]
                for p in persons["@list"] if p["email"] == study["accountablePerson"]
            ][0]
        )
        if "contributor" in study:
            yield (
                "contributor",
                ", ".join([
                    p["name"]
                    for contributor_spec in study["contributor"]
                    for p in persons["@list"] if p["@id"] == contributor_spec["@id"]
                ])
            )

        if publications:
            for publication in publications["@list"]:
                # Emit authors
                yield (
                    "publication.author",
                    ", ".join([
                        p["name"]
                        for author_spec in publication["author"]
                        for p in persons["@list"] if p["@id"] == author_spec["@id"]
                    ])
                )
                yield ("publication.title", publication["headline"])
                yield ("publication.year", publication["datePublished"])

        if "keywords" in study:
            yield ("keywords", ", ".join(study.get("keywords", [])))
