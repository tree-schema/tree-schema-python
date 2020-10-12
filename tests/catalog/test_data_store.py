import requests
import unittest
from unittest.mock import MagicMock, patch

import mock
import pytest
import treeschema
from treeschema.catalog import DataStore, TreeSchemaUser

from . import TEST_USER

class TestDataStore(unittest.TestCase):

    data_store_inputs = {
        'created_ts': '2020-01-01 00:00:00',
        'data_store_id': 1,
        'description_markup': None,
        'description_raw': None,
        'details': {},
        'name': 'Test DS',
        'other_type': None,
        'steward': TEST_USER,
        'tech_poc': TEST_USER,
        'type': 'kafka',
        'updated_ts': '2020-01-01 00:00:00'
    }

    def test_data_store_fields(self):
        assert sorted(list(self.data_store_inputs)) == sorted(list(DataStore.__FIELDS__.keys()))
    
    def test_create_data_store(self):
        DataStore(self.data_store_inputs)

    @patch('treeschema.api.client.r.get')  
    def test_schemas_access(self, mock_get):
        test_obj = {'meta': {'next_page': None}, 'data_schemas': []}
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = test_obj
        mock_get.return_value = response

        ds = DataStore(self.data_store_inputs)
        ds._schemas_by_id is ds.schemas

    @patch('treeschema.api.client.r.get')  
    def test_add_remove_schemas(self, mock_get):
        test_obj = {'meta': {'next_page': None}, 'data_schemas': []}
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = test_obj
        mock_get.return_value = response

        ds = DataStore(self.data_store_inputs)
        
        m = mock.Mock()
        m.id = 3
        m.name = 'schema_name'

        assert ds.schemas == {}
        assert ds._schemas_by_id == {}
        assert ds._schemas_by_name == {}

        ds._add_data_schema(m)

        assert 3 in ds._schemas_by_id.keys()
        assert 'schema_name' in ds._schemas_by_name.keys()

        ds._remove_data_schema(3)

        assert ds.schemas == {}
        assert ds._schemas_by_id == {}
        assert ds._schemas_by_name == {}
