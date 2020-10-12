import requests
import unittest
from unittest.mock import MagicMock, patch

import mock
import pytest
import treeschema
from treeschema.catalog import DataField, TreeSchemaUser

from . import TEST_USER

class TestDataField(unittest.TestCase):

    data_field_inputs = {
        'created_ts': '2020-01-01 00:00:00',
        'description_markup': None,
        'description_raw': None,
        'data_type': 'string',
        'data_format': 'YYYY-MM-DD',
        'field_id': 1,
        'full_path_name': 'full.path.field.name',
        'name': str,
        'nullable': bool,
        'parent_path': None,
        'steward': TEST_USER,
        'tech_poc': TEST_USER,
        'type': 'scalar',
        'updated_ts': '2020-01-01 00:00:00'
    }

    def test_data_field_fields(self):
        assert sorted(list(self.data_field_inputs.keys())) == sorted(list(DataField.__FIELDS__.keys()))
    
    def test_create_data_field(self):
        DataField(self.data_field_inputs, data_store_id=1, data_schema_id=1)

    @patch('treeschema.api.client.r.get')  
    def test_field_values_access(self, mock_get):
        test_obj = {'meta': {'next_page': None}, 'field_values': []}
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = test_obj
        mock_get.return_value = response

        df = DataField(self.data_field_inputs, data_store_id=1, data_schema_id=1)
        df._field_values_by_id is df.field_values

    @patch('treeschema.api.client.r.get')  
    def test_add_remove_field_values(self, mock_get):
        test_obj = {'meta': {'next_page': None}, 'field_values': []}
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = test_obj
        mock_get.return_value = response

        df = DataField(self.data_field_inputs, data_store_id=1, data_schema_id=1)
        
        m = mock.Mock()
        m.id = 3
        m.field_value = 'field_value'

        assert df.field_values == {}
        assert df._field_values_by_id == {}
        assert df._field_values_by_value == {}

        df._add_field_value(m)

        assert 3 in df._field_values_by_id.keys()
        assert 'field_value' in df._field_values_by_value.keys()

        df._remove_field_value(3)

        assert df.field_values == {}
        assert df._field_values_by_id == {}
        assert df._field_values_by_value == {}

    def test_invlaid_field_inputs(self):
        df = DataField(self.data_field_inputs, data_store_id=1, data_schema_id=1)
        # Invalid type1
        invalid_types_tuple = {
            'type': tuple,
            'data_type': tuple,
            'data_format': tuple
        }
        with pytest.raises(treeschema.exceptions.InvalidFieldInputs):
            df._clean_field_inputs(invalid_types_tuple)

        invalid_types_string = {
            'type': 'string',
            'data_type': 'string',
            'data_format': 'string'
        }
        with pytest.raises(treeschema.exceptions.InvalidFieldInputs):
            df._clean_field_inputs(invalid_types_string)

    def test_clean_field_inputs(self):
        df = DataField(self.data_field_inputs, data_store_id=1, data_schema_id=1)
        
        # Native string type
        str_types = {
            'type': str,
            'data_type': str,
            'data_format': str
        }
        df._clean_field_inputs(str_types)
        expected_str_output = {
            'type': 'scalar',
            'data_type': 'string',
            'data_format': 'str'
        }
        assert str_types == expected_str_output

        # Single string type
        single_str_type = {
            'type': str
        }
        df._clean_field_inputs(single_str_type)
        expected_single_str_output = {
            'type': 'scalar',
            'data_type': 'string',
            'data_format': 'str'
        }
        assert single_str_type == expected_single_str_output

        # Native int type
        int_types = {
            'type': int,
            'data_type': int,
            'data_format': int
        }
        df._clean_field_inputs(int_types)
        expected_int_output = {
            'type': 'scalar',
            'data_type': 'number',
            'data_format': 'int'
        }
        assert int_types == expected_int_output

        # Single int type
        single_int_type = {
            'type': int
        }
        df._clean_field_inputs(single_int_type)
        expected_single_int_output = {
            'type': 'scalar',
            'data_type': 'number',
            'data_format': 'int'
        }
        assert single_int_type == expected_single_int_output

        # Native float type
        float_types = {
            'type': float,
            'data_type': float,
            'data_format': float
        }
        df._clean_field_inputs(float_types)
        expected_float_output = {
            'type': 'scalar',
            'data_type': 'number',
            'data_format': 'float'
        }
        assert float_types == expected_float_output

        # Single float type
        single_float_type = {
            'type': float
        }
        df._clean_field_inputs(single_float_type)
        expected_single_float_output = {
            'type': 'scalar',
            'data_type': 'number',
            'data_format': 'float'
        }
        assert single_float_type == expected_single_float_output

        # Native bytes type
        bytes_types = {
            'type': bytes,
            'data_type': bytes,
            'data_format': bytes
        }
        df._clean_field_inputs(bytes_types)
        expected_bytes_output = {
            'type': 'scalar',
            'data_type': 'bytes',
            'data_format': 'bytes'
        }
        assert bytes_types == expected_bytes_output

        # Single bytes type
        single_bytes_type = {
            'type': bytes
        }
        df._clean_field_inputs(single_bytes_type)
        expected_single_bytes_output = {
            'type': 'scalar',
            'data_type': 'bytes',
            'data_format': 'bytes'
        }
        assert single_bytes_type == expected_single_bytes_output

        # Native boolean type
        bool_types = {
            'type': bool,
            'data_type': bool,
            'data_format': bool
        }
        df._clean_field_inputs(bool_types)
        expected_bool_output = {
            'type': 'scalar',
            'data_type': 'boolean',
            'data_format': 'bool'
        }
        assert bool_types == expected_bool_output

        # Single boolean type
        single_boolean_type = {
            'type': bool
        }
        df._clean_field_inputs(single_boolean_type)
        expected_single_boolean_output = {
            'type': 'scalar',
            'data_type': 'boolean',
            'data_format': 'bool'
        }
        assert single_boolean_type == expected_single_boolean_output

        # Native list type
        list_types = {
            'type': list,
            'data_type': list,
            'data_format': list
        }
        df._clean_field_inputs(list_types)
        expected_list_output = {
            'type': 'list',
            'data_type': 'array',
            'data_format': 'list'
        }
        assert list_types == expected_list_output

        # Single list type
        single_list_type = {
            'type': list
        }
        df._clean_field_inputs(single_list_type)
        expected_single_list_output = {
            'type': 'list',
            'data_type': 'array',
            'data_format': 'list'
        }
        assert single_list_type == expected_single_list_output

        # Native dict type
        dict_types = {
            'type': dict,
            'data_type': dict,
            'data_format': dict
        }
        df._clean_field_inputs(dict_types)
        expected_dict_output = {
            'type': 'object',
            'data_type': 'object',
            'data_format': 'dict'
        }
        assert dict_types == expected_dict_output

        # Single list type
        single_dict_type = {
            'type': dict
        }
        df._clean_field_inputs(single_dict_type)
        expected_single_dict_output = {
            'type': 'object',
            'data_type': 'object',
            'data_format': 'dict'
        }
        assert single_dict_type == expected_single_dict_output

        ##############################################
        # Test A Type and a format, but no data type #
        ##############################################
        diff_type_and_fmt = {
            'type': str,
            'data_format': 'YYYY-MM-DD'
        }
        df._clean_field_inputs(diff_type_and_fmt)
        expected_diff_type_and_fmt = {
            'type': 'scalar',
            'data_type': 'string',
            'data_format': 'YYYY-MM-DD'
        }
        assert diff_type_and_fmt == expected_diff_type_and_fmt
