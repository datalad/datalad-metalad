"""Common helper functionality for metalad-based and extension extractors """

from datalad.utils import ensure_unicode
from pathlib import Path

def get_text_from_file(file_path:Path):
    """Return content of a text file as a string"""
    # TODO: check that file is text-based
    file_text = None
    try:
        with open(file_path) as f:
            file_text = ensure_unicode(f.read()).strip()
            return file_text
    except FileNotFoundError as e:
        # TODO: consider returning None in case of exception, depending
        # on what extractors would expect as default behaviour
        print((f'The provided file path could not be found: {str(file_path)}'))
        raise 