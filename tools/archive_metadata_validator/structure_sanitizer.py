from typing import Dict, List


top = [
    "study",
    "dataset",
    "person",
    "publication",
    "additional_information"
]


possible_containments = {
    "study": {
        "name": None,
        "principal_investigator": None,
        "keyword": None,
        "funding": None,
        "purpose": None,
        "start_date": None,
        "end_date": None,
        "contributor": None
    },
    "dataset": {
        "name": None,
        "location": None,
        "author": None,
        "keyword": None,
        "description": None,
        "funding": None,
        "standard": None
    },
    "person": {
        "<email>": {
            "given_name": None,
            "last_name": None,
            "orcid-id": None,
            "title": None,
            "affiliation": None,
            "contact_information": None
        }
    },
    "publication": {
        "title": None,
        "author": None,
        "year": None,
        "corresponding_author": None,
        "doi": None,
        "publication": None,
        "volume": None,
        "issue": None,
        "pages": None,
        "publisher": None
    },
    "additional_information": None
}


def possible_pathes(info: dict, prefix: str = "") -> List[str]:
    result = []
    for key, value in info.items():
        if isinstance(value, dict):
            result += possible_pathes(value, (prefix if prefix is "" else prefix + ".") + key)
        else:
            result += [(prefix if prefix is "" else prefix + ".") + key]
    return result


def possible_container(info: dict,  result: Dict[str, List[str]], container: str = ""):
    for key, value in info.items():
        if key in result:
            result[key].append(container)
        else:
            result[key] = [container]
        if isinstance(value, dict):
            possible_container(value, result, key)
    return


def categorize_token(token: str) -> str:
    parts = token.split("@")
    if len(parts) == 2 and len(parts[0]) > 0 and len(parts[1]) > 0:
        return f"<email value='{token}'>"
    return token


def get_context_key(context: List[str]) -> str:
    return ".".join(context)


