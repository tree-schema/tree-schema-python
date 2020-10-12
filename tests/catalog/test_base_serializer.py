import unittest

import mock
import pytest

import treeschema
from treeschema.catalog.base_serializer import TreeSchemaSerializer


class TestBaseSerializer(unittest.TestCase):

    def test_validate_input(self):
        with mock.patch.object(TreeSchemaSerializer, 'obj'):
            inputs = {}
            serializer = TreeSchemaSerializer(inputs)
            with pytest.raises(treeschema.exceptions.InvalidInputs):
                serializer._validate_input([])
                serializer._validate_input((1,))
                serializer._validate_input({1})
            serializer._validate_input({'some': 'val'})
            serializer._validate_input(1)
            serializer._validate_input('object_name')

    def test_serialize_obj(self):
        with mock.patch.object(TreeSchemaSerializer, 'obj'):
            fields = {'some_name': str, 'some_id': int}
            to_serialize = {'some_name': 'this_name', 'some_id': 3}
            serializer = TreeSchemaSerializer({})
            serializer.__FIELDS__ = fields
            serializer._serialize_obj(to_serialize)

            assert hasattr(serializer, 'some_name')
            assert serializer.some_name == 'this_name'
            assert hasattr(serializer, 'some_id')
            assert serializer.some_id == 3
            
    def test_all_valid_inputs(self):
        with mock.patch.object(TreeSchemaSerializer, 'obj'):
            reqd_fields = {'some_name': str, 'some_id': int, 'another_field': str}
            serializer = TreeSchemaSerializer({})
            serializer.__FIELDS__ = reqd_fields

            valid_obj = {'some_name': 'this_name', 'some_id': 3, 'another_field': 'x'}
            valid_obj_resp = serializer._all_valid_inputs(valid_obj)
            assert valid_obj_resp == True

            invalid_obj = {'some_name': 'this_name'}
            invalid_obj_resp = serializer._all_valid_inputs(invalid_obj)
            assert invalid_obj_resp == False
            
    def test_scalar_or_entity_id(self):
        with mock.patch.object(TreeSchemaSerializer, 'obj'):
            serializer = TreeSchemaSerializer({})
            serializer.id = 1

            scalar_from_obj = serializer._scalar_or_entity_id(serializer) 
            scalar_from_id = serializer._scalar_or_entity_id(1)
            
            assert scalar_from_obj == scalar_from_id
            
