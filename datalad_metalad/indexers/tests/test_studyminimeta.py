from datalad.tests.utils_pytest import eq_

from .common import MOCKED_STUDYMINIMETA_JSONLD
from ..studyminimeta import STUDYMINIMETA_FORMAT_NAME, StudyMiniMetaIndexer


def test_studyminimeta_indexing():

    # Check proper indexing of JSON-LD study-minimeta description

    indexer = StudyMiniMetaIndexer(STUDYMINIMETA_FORMAT_NAME)
    generated_dict = dict(indexer.create_index(MOCKED_STUDYMINIMETA_JSONLD))
    template_dict = {
        'dataset.author': 'Prof. Dr.  Alex Meyer, MD  Bernd Muller',
        'dataset.name': 'Datenddaslk',
        'dataset.location': 'http://dlksdfs.comom.com',
        'dataset.description': 'Some data I collected once upon a time, '
                               'spending hours and hours in dark places.',
        'dataset.keywords': 'd_k1, d_k2, d_k3',
        'dataset.standards': 'dicom, ebdsi',
        'dataset.funder': 'ds-funder-1, ds-funder-2',
        'person.email': 'a@example.com, b@example.com',
        'person.name': 'Prof. Dr.  Alex Meyer, MD  Bernd Muller',
        'name': 'A small study',
        'accountable_person': 'Prof. Dr.  Alex Meyer',
        'contributor': 'Prof. Dr.  Alex Meyer, MD  Bernd Muller',
        'publication.author': 'Prof. Dr.  Alex Meyer, MD  Bernd Muller',
        'publication.title': 'Publication Numero Quatro',
        'publication.year': 2004,
        'keywords': 'k1, k2',
        'funder': 'DFG, NHO'
    }
    eq_(generated_dict, template_dict)
