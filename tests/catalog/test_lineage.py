import requests
import unittest
from unittest.mock import MagicMock, patch

import mock
import pytest
import treeschema
from treeschema.catalog import lineage
from treeschema import exceptions as ts_exceptions

from . import TEST_USER

class TestLineageImpactSummary(unittest.TestCase):

    VALID_INPUTS = {
        'data_stores': 1,
        'schemas': 3,
        'fields': 4
    }
        

    def test_impact_summary_creation(self):
        with pytest.raises(ts_exceptions.InvalidInputs):
            # One invalid link
            invalid_1 = {
                'some_rand_field': 1
            }
            lineage.LineageImpactSummary(invalid_1)

        with pytest.raises(ts_exceptions.InvalidInputs):
            invalid_2 = {
                'data_stores': 'str',
                'schemas': 3,
                'fields': 4
            }
            lineage.LineageImpactSummary(invalid_2)

        assert lineage.LineageImpactSummary(self.VALID_INPUTS)
    
    def test_to_dict(self):
        vi = lineage.LineageImpactSummary(self.VALID_INPUTS)
        assert vi.__dict__() == vi._impact_summary_raw



class TestImpactedAsset(unittest.TestCase):

    VALID_INPUTS = {
        'data_store_id': 1,
        'schema_id': 3,
        'field_id': 4
    }

    def test_impacted_asset_creation(self):
        with pytest.raises(ts_exceptions.InvalidInputs):
            # One invalid link
            invalid_1 = {
                'some_rand_field': 1
            }
            lineage.ImpactedAsset(invalid_1)

        with pytest.raises(ts_exceptions.InvalidInputs):
            invalid_2 = {
                'data_store_id': 'str',
                'schema_id': 3,
                'field_id_not_correct': 4
            }
            lineage.ImpactedAsset(invalid_2)

        assert lineage.ImpactedAsset(self.VALID_INPUTS)
    
    def test_to_dict(self):
        vi = lineage.ImpactedAsset(self.VALID_INPUTS)
        assert vi.__dict__() == vi._raw_asset


    def test_build_full_asset_list(self):
        input_val = {
            'data_store_id': 1,
            'schema_id': 3,
            'field_id': 4,
            'impact_chain': [
                {
                    'data_store_id': 7,
                    'schema_id': 8,
                    'field_id': 9
                },
                {
                    'data_store_id': 10,
                    'schema_id': 11,
                    'field_id': 12
                },
                {
                    'data_store_id': 13,
                    'schema_id': 14,
                    'field_id': 15
                },
            ]
        }
        li = lineage.ImpactedAsset(input_val)
        li._build_full_asset_list()

        expected_output = [
            {
                'data_store_id': 7,
                'schema_id': 8,
                'field_id': 9
            },
            {
                'data_store_id': 10,
                'schema_id': 11,
                'field_id': 12
            },
            {
                'data_store_id': 13,
                'schema_id': 14,
                'field_id': 15
            },
            {
                'data_store_id': 1,
                'schema_id': 3,
                'field_id': 4
            }
        ]
        for i, dict_val in enumerate(expected_output):
            assert dict_val == li._all_raw_assets[i]


class TestLineageImpact(unittest.TestCase):

    VALID_INPUTS = {
        'data_store_id': 1,
        'schema_id': 3,
        'field_id': 4
    }

    def test_impacted_asset_creation(self):
        with pytest.raises(ts_exceptions.InvalidInputs):
            # One invalid link
            invalid_1 = {
                'some_rand_field': 1
            }
            lineage.ImpactedAsset(invalid_1)

        with pytest.raises(ts_exceptions.InvalidInputs):
            invalid_2 = {
                'data_store_id': 'str',
                'schema_id': 3,
                'field_id_not_correct': 4
            }
            lineage.ImpactedAsset(invalid_2)

        assert lineage.ImpactedAsset(self.VALID_INPUTS)
    
    def test_to_dict(self):
        vi = lineage.ImpactedAsset(self.VALID_INPUTS)
        assert vi.__dict__() == vi._raw_asset

 