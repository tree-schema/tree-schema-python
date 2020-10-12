import requests
import unittest
from unittest.mock import MagicMock, patch

import mock
import pytest

import treeschema
from treeschema.catalog import TreeSchemaUser
from . import TEST_TREE_SCHEMA

class TestTreeSchema(unittest.TestCase):

    @patch('treeschema.api.client.r.get')  
    def test_get_users(self, mock_get):
        resp_users = [
            {
                "user_id": 2,
                "name": "Asher",
                "email": "asher@treeschema.com"
            },
            {
                "user_id": 1,
                "name": "Grant",
                "email": "grant@treeschema.com"
            }
        ]
        resp_obj = {
            "meta": {
                "current_page": 1,
                "next_page": None,
                "total_cnt": 2
            },
            "users": resp_users
        }
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        TEST_TREE_SCHEMA.get_users()
        
        usrs_by_id = TEST_TREE_SCHEMA._entity_holder._users_by_id.keys()
        usrs_by_email = TEST_TREE_SCHEMA._entity_holder._users_by_email.keys()
        
        assert 1 in usrs_by_id
        assert 2 in usrs_by_id

        assert 'asher@treeschema.com' in usrs_by_email
        assert 'grant@treeschema.com' in usrs_by_email

    @patch('treeschema.api.client.r.get')  
    def test_get_all_data_stores(self, mock_get):
        ds_name = "Kafka Prod Cluster"
        resp_data_stores = [
            {
                "data_store_id": 18,
                "name": ds_name,
                "type": "kafka",
                "other_type": None,
                "created_ts": "2020-09-23 18:16:16",
                "updated_ts": "2020-09-23 18:16:16",
                "description_markup": "<p>This is the Kafka cluster.</p>",
                "description_raw": "This is the Kafka cluster.",
                "steward": {
                    "user_id": 1,
                    "name": "Grant",
                    "email": "grant@treeschema.com"
                },
                "tech_poc": {
                    "user_id": 2,
                    "name": "Asher",
                    "email": "asher@treeschema.com"
                },
                "details": {
                    "bootstrap_servers": "1.3.5.7:22"
                }
            }
        ]
        resp_obj = {
            "meta": {
                "current_page": 1,
                "next_page": None,
                "total_cnt": 2
            },
            "data_stores": resp_data_stores
        }
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        TEST_TREE_SCHEMA.get_data_stores()

        ds_by_id = TEST_TREE_SCHEMA._entity_holder._data_stores_by_id.keys()
        ds_by_name = TEST_TREE_SCHEMA._entity_holder._data_stores_by_name.keys()
        
        assert 18 in ds_by_id
        assert ds_name.lower() in ds_by_name
        

    @patch('treeschema.api.client.r.get')  
    def test_get_all_transformations(self, mock_get):
        transformation_name = "My Tansform"
        resp_transformations = [
            {
                "transformation_id": 25,
                "name": transformation_name,
                "type": "batch_process_triggered",
                "created_ts": "2020-09-22 17:20:38",
                "updated_ts": "2020-09-22 17:25:34",
                "description_markup": "<p>desc</p>",
                "description_raw": "desc",
                "steward": {
                    "user_id": 1,
                    "name": "Grant",
                    "email": "grant@treeschema.com"
                },
                "tech_poc": {
                    "user_id": 1,
                    "name": "Grant",
                    "email": "grant@treeschema.com"
                }
            }
        ]
        resp_obj = {
            "meta": {
                "current_page": 1,
                "next_page": None,
                "total_cnt": 2
            },
            "transformations": resp_transformations
        }
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        TEST_TREE_SCHEMA.get_transformations()

        t_by_id = TEST_TREE_SCHEMA._entity_holder._transformations_by_id.keys()
        t_by_name = TEST_TREE_SCHEMA._entity_holder._transformations_by_name.keys()
        
        assert 25 in t_by_id
        assert transformation_name.lower() in t_by_name

