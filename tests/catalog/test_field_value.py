import unittest

import mock
import pytest
import treeschema
from treeschema.catalog import FieldValue, TreeSchemaUser


class TestFieldValue(unittest.TestCase):

    field_value_inputs = {
        'created_ts': '2020-01-01 00:00:00',
        'description_markup': None,
        'description_raw': None,
        'field_value': 'some-value',
        'field_value_id': 1,
        'updated_ts': '2020-01-01 00:00:00'
    }

    def test_data_field_fields(self):
        assert sorted(list(self.field_value_inputs.keys())) == sorted(list(FieldValue.__FIELDS__.keys()))
    
