
PERSON_SPEC = {
    "a@example.com": {
        "given_name": "gn-a",
        "last_name": "ln-a"
    },
    "b@example.com": {
        "given_name": "gn-b",
        "last_name": "ln-b"
    }
}

BASE_SPEC = {
    "person": PERSON_SPEC,
    "study": {
        "principal_investigator": "a@example.com"
    },
    "dataset": {
        "author": "a@example.com",
    },
    "publication": [
        {
            "title": "Publication One",
            "author": [
                "a@example.com",
                "b@example.com"
            ],
            "corresponding_author": "a@example.com",
        }
    ]
}


MINIMAL_SPEC = {
    "person": PERSON_SPEC,
    "study": {
        "principal_investigator": "a@example.com"
    },
    "dataset": {
        "author": "a@example.com",
    }
}
