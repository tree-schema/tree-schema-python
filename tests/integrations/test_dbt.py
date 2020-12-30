import os
import requests
import unittest
from unittest.mock import MagicMock, patch

import mock
import pytest
import treeschema
from treeschema.integrations.dbt import DbtManager


class TestDbt(unittest.TestCase):

    dbt_inputs = {'data_store_id': 1}

    def test_could_not_find_dbt_file(self):
        dbt = DbtManager(**self.dbt_inputs)
        with pytest.raises(treeschema.exceptions.DbtManifestInvalid):
            resp = dbt.parse_dbt_manifest('../test_data/dbt/manifest.json')

    def test_invalid_manifest_content(self):
        dbt = DbtManager(**self.dbt_inputs)
        with pytest.raises(treeschema.exceptions.DbtManifestInvalid):
            resp = dbt.parse_dbt_manifest(b'some invalid content')

    @patch('treeschema.api.client.r.post')  
    def test_parse_dbt_interface(self, mock_post):
        test_obj = {'dbt_process_id': 'abc-123'}
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = test_obj
        mock_post.return_value = response

        dbt = DbtManager(**self.dbt_inputs)
        file_loc = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
            'test_data/dbt/manifest.json'
        )
        resp = dbt.parse_dbt_manifest(file_loc)
        assert resp == test_obj['dbt_process_id']

    @patch('treeschema.api.client.r.get')  
    def test_parse_dbt_interface(self, mock_get):
        test_obj = {
            'status': 'success',
            'error_msg': None,
            'dbt_schemas': [{'schema_name': 'abc', 'schema_type': 'table'}],
            'dbt_lineage': [
                {'source_schema_name': 'src', 'target_schema_name': 'tgt'}
            ],
        }
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = test_obj
        mock_get.return_value = response

        dbt = DbtManager(**self.dbt_inputs)
        dbt.dbt_process_id = 'abc'
        dbt.get_manifest_parse_status()

        assert dbt.parsed_schemas == test_obj['dbt_schemas']
        assert dbt.parsed_lineage == test_obj['dbt_lineage']

