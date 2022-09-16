# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 et:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Legacy metadata extractors"""

from os.path import join

from datalad.consts import DATALAD_DOTDIR

METADATA_DIR = join(DATALAD_DOTDIR, 'metadata')
DATASET_METADATA_FILE = join(METADATA_DIR, 'dataset.json')
DATASET_CONFIG_FILE = join(DATALAD_DOTDIR, 'config')
