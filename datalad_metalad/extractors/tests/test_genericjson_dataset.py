# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil; coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test generic json dataset-level metadata extractor"""

import json

from datalad.distribution.dataset import Dataset
from datalad.tests.utils_pytest import (
    assert_in_results,
    assert_raises,
    assert_repo_status,
    assert_result_count,
    assert_status,
    assert_equal,
    known_failure_windows,
    with_tree,
)


# some metadata to play with, taken from the examples of the google dataset
# search
# this is JSON+LD with plain schema.org terms
sample_jsonld = \
{
    "@context": "https://schema.org/",
    "@type": "Dataset",
    "name": "NCDC Storm Events Database",
    "description": "Storm Data is provided by the National Weather Service (NWS) and contain statistics on...",
    "url": "https://catalog.data.gov/dataset/ncdc-storm-events-database",
    "sameAs": "https://gis.ncdc.noaa.gov/geoportal/catalog/search/resource/details.page?id=gov.noaa.ncdc:C00510",
    "keywords": [
        "ATMOSPHERE > ATMOSPHERIC PHENOMENA > CYCLONES",
        "ATMOSPHERE > ATMOSPHERIC PHENOMENA > DROUGHT",
        "ATMOSPHERE > ATMOSPHERIC PHENOMENA > FOG",
        "ATMOSPHERE > ATMOSPHERIC PHENOMENA > FREEZE"
    ],
    "creator": {
        "@type": "Organization",
        "url":  "https://www.ncei.noaa.gov/",
        "name": "OC/NOAA/NESDIS/NCEI > National Centers for Environmental Information, NESDIS, NOAA, U.S. Department of Commerce",
        "contactPoint": {
            "@type": "ContactPoint",
            "contactType": "customer service",
            "telephone": "+1-828-271-4800",
            "email": "ncei.orders@noaa.gov"
        }
    },
    "includedInDataCatalog": {
        "@type": "DataCatalog",
        "name": "data.gov"
    },
    "distribution": [
        {
            "@type": "DataDownload",
            "encodingFormat": "CSV",
            "contentUrl": "http://www.ncdc.noaa.gov/stormevents/ftp.jsp"
        },
        {
            "@type": "DataDownload",
            "encodingFormat": "XML",
            "contentUrl": "http://gis.ncdc.noaa.gov/all-records/catalog/search/resource/details.page?id=gov.noaa.ncdc:C00510"
        }
    ],
    "temporalCoverage": "1950-01-01/2013-12-18",
    "spatialCoverage": {
        "@type": "Place",
        "geo": {
            "@type": "GeoShape",
            "box": "18.0 -65.0 72.0 172.0"
        }
    }
}

testmeta = {
    "@id": "magic",
    "name": "silence"
}


@with_tree(
    tree={
        '.metadata': {
            'dataset.json': json.dumps(sample_jsonld)},
        'down': {
            'customloc': json.dumps(testmeta)}})
def test_custom_dsmeta(path=None):
    ds = Dataset(path).create(force=True)
    sample_jsonld_ = dict(sample_jsonld)
    
    # 1. Test default source location (no configured source)
    # -> extraction yields ok result
    ds.save(result_renderer="disabled")
    assert_repo_status(ds.path)
    res = ds.meta_extract(extractorname='metalad_genericjson_dataset')
    assert_status('ok', res)
    assert_result_count(res, 1)
    dsmeta = res[0]
    assert_equal(sample_jsonld_, dsmeta['metadata_record']['extracted_metadata'])

    # 2. Test configured source location, but non-existing file
    # -> extraction yields a failure result
    res = ds.meta_extract(
            extractorname='metalad_genericjson_dataset',
            extractorargs=['metadata_source', 'nothere'],
            # force_dataset_level=True,
            result_renderer='disabled',
            on_failure='ignore'
            )
    assert_in_results(
        res,
        action="meta_extract",
        status="impossible",
        message=(
            'metadata source is not available in %s: %s',
            ds.path, 'nothere'),
        path=ds.path,
    )
    assert_result_count(
        res, 1, action='meta_extract', type='dataset', status='impossible',
        path=ds.path,
        message=(
            'metadata source is not available in %s: %s',
            ds.path, 'nothere'),
    )
    assert_equal(res[0].get('metadata_record', {}), {})

    # 3. Test configured source location, file exists
    # -> extraction yields ok result
    res = ds.meta_extract(
        extractorname='metalad_genericjson_dataset',
        extractorargs=['metadata_source', 'down/customloc'],
        # force_dataset_level=True,
        on_failure='ignore',
        result_renderer="disabled")
    assert_result_count(res, 1)
    assert_status('ok', res)
    assert_equal(testmeta, res[0]['metadata_record']['extracted_metadata'])

    # 4. Test multiple configured source location, file exists
    res = ds.meta_extract(
        extractorname='metalad_genericjson_dataset',
        extractorargs=['metadata_source', '["down/customloc", ".metadata/dataset.json"]'],
        on_failure='ignore',
        result_renderer="disabled")
    assert_result_count(res, 1)
    assert_equal(
        # merge order: testmeta <- sample_jsonld
        dict(testmeta, **sample_jsonld),
        res[0]['metadata_record']['extracted_metadata']
    )
