import requests
import unittest
from unittest.mock import MagicMock, patch

import mock
import pytest
import treeschema
from treeschema.catalog import DataField, Transformation, TreeSchemaUser

from . import TEST_USER
from .test_data_field import TestDataField

class TestTransformation(unittest.TestCase):

    transformation_inputs = {     
        'created_ts': '2020-01-01 00:00:00',   
        'description_markup': None,
        'description_raw': None,
        'name': 'Test DS',
        'steward': TEST_USER,
        'tech_poc': TEST_USER,
        'transformation_id': 1,
        'type': 'pub_sub_event',
        'updated_ts': '2020-01-01 00:00:00'
    }

    def test_transformation_fields(self):
        assert sorted(list(self.transformation_inputs)) == sorted(list(Transformation.__FIELDS__.keys()))
    
    def test_create_transformation(self):
        Transformation(self.transformation_inputs)

    @patch('treeschema.api.client.r.get')  
    def test_transformation_link_access(self, mock_get):
        test_obj = {'meta': {'next_page': None}, 'transformation_links': []}
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = test_obj
        mock_get.return_value = response
        
        t = Transformation(self.transformation_inputs)
        t._links_by_id is t.links

    @patch('treeschema.api.client.r.get')  
    def test_add_remove_links(self, mock_get):
        test_obj = {'meta': {'next_page': None}, 'transformation_links': []}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = test_obj
        mock_get.return_value = response
        

        t = Transformation(self.transformation_inputs)
        
        m = mock.Mock()
        m.id = 3
        m.name = 'link_id'

        assert t.links == {}
        assert t._links_by_id == {}

        t._add_link(m)

        assert 3 in t._links_by_id.keys()

        t._remove_link(3)

        assert t.links == {}
        assert t._links_by_id == {}

    def test_is_transform_link_obj(self):
        t = Transformation(self.transformation_inputs)

        # Test as tuple and list
        link_obj_1 = {'source_field_id': 1, 'target_field_id': 2}
        assert t._is_transform_link_obj(link_obj_1) == True
        
        # Invalid data field tuple objects
        link_obj_2 = {'source_field_id': 1, 'target_field_id': 2, 'another_field': 3}
        assert t._is_transform_link_obj(link_obj_2) == False

        link_obj_3 = {'target_field_id': 2}
        assert t._is_transform_link_obj(link_obj_3) == False

    def test_is_transform_field_obj(self):
        t = Transformation(self.transformation_inputs)

        # Valid data field tuple object
        inputs_1 = TestDataField.data_field_inputs.copy()
        inputs_2 = TestDataField.data_field_inputs.copy()
        inputs_2['field_id'] = 5
        
        df1 = DataField(inputs_1, data_store_id=1, data_schema_id=1)
        df2 = DataField(inputs_2, data_store_id=1, data_schema_id=1)
        
        # Test as tuple and list
        link_obj_1a = (df1, df2)
        assert t._is_transform_field_obj(link_obj_1a) == True
        link_obj_1b = [df1, df2]
        assert t._is_transform_field_obj(link_obj_1b) == True

        # Invalid data field tuple objects
        link_obj_2 = (df1, 3)
        assert t._is_transform_field_obj(link_obj_2) == False

        link_obj_3 = (df1, df2, 1)
        assert t._is_transform_field_obj(link_obj_3) == False

    def test_get_link_structure(self):
        t = Transformation(self.transformation_inputs)

        # One valid link
        links_1 = {'source_field_id': 1, 'target_field_id': 2}
        links_created_1 = t._get_link_structure(links_1)
        assert links_created_1 == [links_1]
        
        # One invalid link
        invalid_links_1 = {
            'source_field_id': 1
        }
        invalid_links_created_1 = t._get_link_structure(invalid_links_1)
        assert invalid_links_created_1 == None

        # Multiple valid links
        links_2 = [
            {'source_field_id': 1, 'target_field_id': 2},
            {'source_field_id': 3, 'target_field_id': 4}
        ]
        links_created_2 = t._get_link_structure(links_2)
        assert links_created_2 == links_2

        # One valid, one invalid link (all must be valid)
        invalid_links_2 = [
            {'source_field_id': 1, 'target_field_id': 2},
            {'source_field_id': 3}
        ]
        invalid_links_created_2 = t._get_link_structure(invalid_links_2)
        assert invalid_links_created_2 == None
        
        # One valid DataField
        inputs_1 = TestDataField.data_field_inputs.copy()
        inputs_2 = TestDataField.data_field_inputs.copy()
        inputs_2['field_id'] = 5
        
        df1 = DataField(inputs_1, data_store_id=1, data_schema_id=1)
        df2 = DataField(inputs_2, data_store_id=1, data_schema_id=1)
        links_3 = (df1, df2)
        
        links_created_3 = t._get_link_structure(links_3)
        expected_result = [{'source_field_id': 1, 'target_field_id': 5}]
        assert links_created_3 == expected_result

        # Multiple valid DataFields
        inputs_1 = TestDataField.data_field_inputs.copy()
        inputs_2 = TestDataField.data_field_inputs.copy()
        inputs_3 = TestDataField.data_field_inputs.copy()
        inputs_4 = TestDataField.data_field_inputs.copy()
        inputs_2['field_id'] = 5
        inputs_3['field_id'] = 7
        inputs_4['field_id'] = 9
        
        df1 = DataField(inputs_1, data_store_id=1, data_schema_id=1)
        df2 = DataField(inputs_2, data_store_id=1, data_schema_id=1)
        df3 = DataField(inputs_3, data_store_id=1, data_schema_id=1)
        df4 = DataField(inputs_4, data_store_id=1, data_schema_id=1)
        links_4 = [(df1, df2), (df3, df4)]
        
        links_created_4 = t._get_link_structure(links_4)
        expected_result = [
            {'source_field_id': 1, 'target_field_id': 5},
            {'source_field_id': 7, 'target_field_id': 9}
        ]
        assert links_created_4 == expected_result
