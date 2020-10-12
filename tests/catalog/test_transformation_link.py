import unittest

import mock
import pytest
import treeschema
from treeschema.catalog import TransformationLink, TreeSchemaUser


class TestTransformationLink(unittest.TestCase):

    transformation_link_inputs = {
        'created_ts': '2020-01-01 00:00:00',
        'source_data_store_id': 8,
        'source_data_store_name': 'src_data_store',
        'source_schema_id': 4,
        'source_schema_name': 'src_schema',
        'source_field_id': 3,
        'source_field_name': 'src_field',
        'target_data_store_id': 1,
        'target_data_store_name': 'tgt_data_store',
        'target_schema_id': 2,
        'target_schema_name': 'tgt_schema',
        'target_field_id': 1,
        'target_field_name': 'tgt_field',
        'transformation_link_id': 1,
        'updated_ts': '2020-01-01 00:00:00',
    }

    def test_transformation_link_fields(self):
        assert sorted(list(self.transformation_link_inputs.keys())) == sorted(list(TransformationLink.__FIELDS__.keys()))
    
