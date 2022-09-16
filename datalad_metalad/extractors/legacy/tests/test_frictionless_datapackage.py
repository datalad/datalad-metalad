# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 et:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test frictionless datapackage metadata extractor """

import json

from datalad.api import Dataset
from datalad.tests.utils_pytest import (
    assert_equal,
    assert_result_count,
    assert_status,
    with_tree,
)

from ..frictionless_datapackage import FRDPMetadataExtractor


# bits from examples and the specs
@with_tree(tree={'datapackage.json': """
{
  "name": "cpi",
  "title": "Annual Consumer Price Index (CPI)",
  "description": "Annual Consumer Price Index (CPI) for most countries in the world. Reference year is 2005.",
  "license" : {
      "type": "odc-pddl",
      "path": "http://opendatacommons.org/licenses/pddl/"
  },
  "keywords": [ "CPI", "World", "Consumer Price Index", "Annual Data", "The World Bank" ],
  "version": "2.0.0",
  "last_updated": "2014-09-22",
  "contributors": [
    {
      "name": "Joe Bloggs",
      "email": "joe@example.com",
      "web": "http://www.example.com"
    }
  ],
  "author": "Jane Doe <noemail@example.com>"
}
"""})
def test_get_metadata(path=None):
    ds = Dataset(path).create(force=True)

    res = ds.meta_extract('frictionless_datapackage')
    assert_status('ok', res)
    assert_result_count(res, 1)

    meta = res[0]['metadata_record']['extracted_metadata']
    assert_equal(
        json.dumps(meta, sort_keys=True, indent=2),
        """\
{
  "author": "Jane Doe <noemail@example.com>",
  "conformsto": "http://specs.frictionlessdata.io/data-packages",
  "contributors": [
    "Joe Bloggs <joe@example.com> (http://www.example.com)"
  ],
  "description": "Annual Consumer Price Index (CPI) for most countries in the world. Reference year is 2005.",
  "license": "http://opendatacommons.org/licenses/pddl/",
  "name": "cpi",
  "shortdescription": "Annual Consumer Price Index (CPI)",
  "tag": [
    "CPI",
    "World",
    "Consumer Price Index",
    "Annual Data",
    "The World Bank"
  ],
  "version": "2.0.0"
}""")
