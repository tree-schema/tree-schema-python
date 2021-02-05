import json
import requests
import unittest
from unittest.mock import MagicMock, patch

import mock
import pytest

from treeschema.api import APIClient
from treeschema import exceptions as ts_exceptions



class TestAPIClient(unittest.TestCase):

    @patch('treeschema.api.client.r.get')  
    def test_get_obj(self, mock_get):
        test_obj = {'data': 'value'}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = test_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client._get_by_url('/an/endpoint')
        assert resp == test_obj

    @patch('treeschema.api.client.r.get')  
    def test_get_400(self, mock_get):
        response = requests.Response()
        response.status_code = 400
        mock_get.return_value = response
        
        client = APIClient()
        with pytest.raises(ts_exceptions.TreeSchemaApiError):
            client._get_by_url('/not/found/endpoint')

    @patch('treeschema.api.client.r.get')  
    def test_get_paginated(self, mock_get):
        response_objcts = [
            1, 2, 3, 4, 'a', 'b', {'c': 'd'}
        ]
        test_obj = {
            "meta": {
                "current_page": 1,
                "next_page": None,
                "total_cnt": 2
            },
            "data_response": response_objcts
        }
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = test_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client._get_paginated_by_url(
            '/an/endpoint',
            pagininate_resp_key='data_response'
            )
        assert resp == response_objcts

    @patch('treeschema.api.client.r.post')  
    def test_post_to_url(self, mock_get):
        test_obj = {'data': 'value'}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = test_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client._post_to_url(
            '/an/endpoint',
            json_body={'any': 'json'}
        )
        assert resp == test_obj

        response = requests.Response()
        response.status_code = 201
        response.json = MagicMock()
        response.json.return_value = test_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client._post_to_url(
            '/an/endpoint',
            json_body={'any': 'json'}
        )
        assert resp == test_obj

    @patch('treeschema.api.client.r.post')  
    def test_post_400(self, mock_get):
        response = requests.Response()
        response.status_code = 400
        mock_get.return_value = response
        
        client = APIClient()
        with pytest.raises(ts_exceptions.TreeSchemaApiError):
            client._post_to_url(
                '/not/found/endpoint',
                json_body={'some': 'body'}
            )

    @patch('treeschema.api.client.r.delete')  
    def test__delete_by_url(self, mock_get):
        test_obj = {'data': 'value'}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = test_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client._delete_by_url(
            '/an/endpoint',
            json_body={'any': 'json'}
        )
        assert resp == True

        response = requests.Response()
        response.status_code = 400
        response.json = MagicMock()
        response.json.return_value = test_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client._delete_by_url(
            '/an/endpoint',
            json_body={'any': 'json'}
        )
        assert resp == False


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
        
        client = APIClient()
        resp = client.get_all_users()
        assert resp == resp_users

    @patch('treeschema.api.client.r.get')  
    def test_get_user_by_email(self, mock_get):
        resp_user = {
                "user_id": 2,
                "name": "Asher",
                "email": "asher@treeschema.com"
        }
        resp_obj = {"user": resp_user}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_user_by_email('asher@treeschema.com')
        assert resp == resp_obj

    @patch('treeschema.api.client.r.get')  
    def test_get_user(self, mock_get):
        resp_user = {
                "user_id": 2,
                "name": "Asher",
                "email": "asher@treeschema.com"
        }
        resp_obj = {"user": resp_user}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_user_by_email(2)
        assert resp == resp_obj

    @patch('treeschema.api.client.r.get')  
    def test_get_all_data_stores(self, mock_get):
        resp_data_stores = [
            {
                "data_store_id": 18,
                "name": "Kafka Prod Cluster",
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
        
        client = APIClient()
        resp = client.get_all_data_stores()
        assert resp == resp_data_stores



    @patch('treeschema.api.client.r.get')  
    def test_data_store_by_name(self, mock_get):
        resp_data_store = {
            "data_store_id": 18,
            "name": "Kafka Prod Cluster",
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
        resp_obj = {"data_store": resp_data_store}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_data_store_by_name('Kafka Prod Cluster')
        assert resp == resp_obj

    @patch('treeschema.api.client.r.get')  
    def test_data_store_by_id(self, mock_get):
        resp_data_store = {
            "data_store_id": 18,
            "name": "Kafka Prod Cluster",
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
        resp_obj = {"data_store": resp_data_store}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_data_store_by_id(1)
        assert resp == resp_obj


    @patch('treeschema.api.client.r.post')  
    def test_create_data_store(self, mock_get):
        resp_data_store = {
            "data_store_id": 18,
            "name": "Kafka Prod Cluster",
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
        resp_obj = {"data_store": resp_data_store}
        
        response = requests.Response()
        response.status_code = 201
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.create_data_store({'data_store': 'inputs'})
        assert resp == resp_obj

    @patch('treeschema.api.client.r.post')  
    def test_add_tag_to_data_store(self, mock_get):
        tags = ['new_tag', 'second tag']
        resp_statuses = ['added', 'exists']

        resp_data_store = {
            "tags": tags,
            "tag_statuses": resp_statuses
        }

        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_data_store
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.add_tag_to_data_store(1, tags)
        assert resp == resp_data_store

    @patch('treeschema.api.client.r.delete')  
    def test_remove_tag_to_data_store(self, mock_get):
        tags = ['new_tag', 'second tag']
        resp_data_store = {
            "tags_removed": tags
        }

        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_data_store
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.remove_data_store_tags(1, tags)
        assert resp == resp_data_store


    @patch('treeschema.api.client.r.get')  
    def test_get_all_schemas_for_data_store(self, mock_get):
        resp_data_schemas = [
            {
                "data_schema_id": 16,
                "name": "public.session_info",
                "type": "table",
                "schema_loc": "public.session_info",
                "created_ts": "2020-09-23 14:56:02",
                "updated_ts": "2020-09-23 14:56:02",
                "description_markup": None,
                "description_raw": None,
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
                "total_cnt": 1
            },
            "data_schemas": resp_data_schemas
        }

        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_all_schemas_for_data_store(
            data_store_id=1
        )
        assert resp == resp_data_schemas


    @patch('treeschema.api.client.r.get')  
    def test_data_schema_by_name(self, mock_get):
        resp_data_schema = {
            "data_schema_id": 16,
            "name": "public.session_info",
            "type": "table",
            "schema_loc": "public.session_info",
            "created_ts": "2020-09-23 14:56:02",
            "updated_ts": "2020-09-23 14:56:02",
            "description_markup": None,
            "description_raw": None,
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
        resp_obj = {"data_schema": resp_data_schema}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_data_schema_by_name(
            data_store_id=1,
            name='public.session_info'
        )
        assert resp == resp_obj


    @patch('treeschema.api.client.r.get')  
    def test_data_schema_by_id(self, mock_get):
        resp_data_schema = {
            "data_schema_id": 16,
            "name": "public.session_info",
            "type": "table",
            "schema_loc": "public.session_info",
            "created_ts": "2020-09-23 14:56:02",
            "updated_ts": "2020-09-23 14:56:02",
            "description_markup": None,
            "description_raw": None,
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
        resp_obj = {"data_schema": resp_data_schema}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_data_schema_by_id(
            data_store_id=1,
            data_schema_id=1
        )
        assert resp == resp_obj


    @patch('treeschema.api.client.r.post')  
    def test_create_data_schema(self, mock_get):
        resp_data_schema = {
            "data_schema_id": 16,
            "name": "public.session_info",
            "type": "table",
            "schema_loc": "public.session_info",
            "created_ts": "2020-09-23 14:56:02",
            "updated_ts": "2020-09-23 14:56:02",
            "description_markup": None,
            "description_raw": None,
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
        resp_obj = {"data_schema": resp_data_schema}
        
        response = requests.Response()
        response.status_code = 201
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.create_data_schema(
            data_store_id=1,
            data_schema_info={'data_schema': 'inputs'}
        )
        assert resp == resp_obj

    @patch('treeschema.api.client.r.post')  
    def test_add_tag_to_data_schema(self, mock_get):
        tags = ['new_tag', 'second tag']
        resp_statuses = ['added', 'exists']

        resp_data_store = {
            "tags": tags,
            "tag_statuses": resp_statuses
        }

        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_data_store
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.add_tag_to_data_schema(
            data_store_id=1,
            data_schema_id=1,
            tags=tags
        )
        assert resp == resp_data_store

    @patch('treeschema.api.client.r.delete')  
    def test_remove_tag_from_data_schema(self, mock_get):
        tags = ['new_tag', 'second tag']
        resp_data_schema = {
            "tags_removed": tags
        }

        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_data_schema
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.remove_data_schema_tags(
            data_store_id=1,
            data_schema_id=1,
            tags=tags
        )
        assert resp == resp_data_schema

    @patch('treeschema.api.client.r.get')  
    def test_get_all_fields_for_data_schema(self, mock_get):
        resp_data_fields = [
            {
                "field_id": 1,
                "name": "FIRST_NAME",
                "parent_path": None,
                "full_path_name": "FIRST_NAME",
                "type": "scalar",
                "data_type": "string",
                "data_format": "VARCHAR2",
                "nullable": False,
                "created_ts": "2020-08-15 17:16:11",
                "updated_ts": "2020-08-15 17:16:11",
                "description_markup": None,
                "description_raw": None,
                "steward": {
                    "user_id": 1,
                    "name": "Grant Seward",
                    "email": "grant@treeschema.com"
                },
                "tech_poc": {
                    "user_id": 1,
                    "name": "Grant Seward",
                    "email": "grant@treeschema.com"
                }
            }
        ]
        resp_obj = {
            "meta": {
                "current_page": 1,
                "next_page": None,
                "total_cnt": 1
            },
            "data_fields": resp_data_fields
        }

        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_all_fields_for_schema(
            data_store_id=1,
            data_schema_id=1
        )
        assert resp == resp_data_fields

    @patch('treeschema.api.client.r.get')  
    def test_data_field_by_name(self, mock_get):
        resp_data_field = {
            "field_id": 1,
            "name": "FIRST_NAME",
            "parent_path": None,
            "full_path_name": "FIRST_NAME",
            "type": "scalar",
            "data_type": "string",
            "data_format": "VARCHAR2",
            "nullable": False,
            "created_ts": "2020-08-15 17:16:11",
            "updated_ts": "2020-08-15 17:16:11",
            "description_markup": None,
            "description_raw": None,
            "steward": {
                "user_id": 1,
                "name": "Grant Seward",
                "email": "grant@treeschema.com"
            },
            "tech_poc": {
                "user_id": 1,
                "name": "Grant Seward",
                "email": "grant@treeschema.com"
            }
        }
        resp_obj = {"data_field": resp_data_field}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_data_field_by_name(
            data_store_id=1,
            data_schema_id=1,
            name='FIRST_NAME'
        )
        assert resp == resp_obj

    @patch('treeschema.api.client.r.get')  
    def test_data_field_by_id(self, mock_get):
        resp_data_field = {
            "field_id": 1,
            "name": "FIRST_NAME",
            "parent_path": None,
            "full_path_name": "FIRST_NAME",
            "type": "scalar",
            "data_type": "string",
            "data_format": "VARCHAR2",
            "nullable": False,
            "created_ts": "2020-08-15 17:16:11",
            "updated_ts": "2020-08-15 17:16:11",
            "description_markup": None,
            "description_raw": None,
            "steward": {
                "user_id": 1,
                "name": "Grant Seward",
                "email": "grant@treeschema.com"
            },
            "tech_poc": {
                "user_id": 1,
                "name": "Grant Seward",
                "email": "grant@treeschema.com"
            }
        }
        resp_obj = {"data_field": resp_data_field}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_data_field_by_id(
            data_store_id=1,
            data_schema_id=1,
            field_id=1
        )
        assert resp == resp_obj

    @patch('treeschema.api.client.r.post')  
    def test_create_data_field(self, mock_get):
        resp_data_field = {
            "field_id": 1,
            "name": "FIRST_NAME",
            "parent_path": None,
            "full_path_name": "FIRST_NAME",
            "type": "scalar",
            "data_type": "string",
            "data_format": "VARCHAR2",
            "nullable": False,
            "created_ts": "2020-08-15 17:16:11",
            "updated_ts": "2020-08-15 17:16:11",
            "description_markup": None,
            "description_raw": None,
            "steward": {
                "user_id": 1,
                "name": "Grant Seward",
                "email": "grant@treeschema.com"
            },
            "tech_poc": {
                "user_id": 1,
                "name": "Grant Seward",
                "email": "grant@treeschema.com"
            }
        }
        resp_obj = {"data_field": resp_data_field}
        
        response = requests.Response()
        response.status_code = 201
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.create_data_field(
            data_store_id=1,
            data_schema_id=1,
            data_field_info={'data_schema': 'inputs'}
        )
        assert resp == resp_obj

    @patch('treeschema.api.client.r.post')  
    def test_update_field(self, mock_get):
        resp_data_field = {
            "field_id": 1,
            "name": "FIRST_NAME",
            "parent_path": None,
            "full_path_name": "FIRST_NAME",
            "type": "scalar",
            "data_type": "string",
            "data_format": "VARCHAR2",
            "nullable": False,
            "created_ts": "2020-08-15 17:16:11",
            "updated_ts": "2020-08-15 17:16:11",
            "description_markup": None,
            "description_raw": None,
            "steward": {
                "user_id": 1,
                "name": "Grant Seward",
                "email": "grant@treeschema.com"
            },
            "tech_poc": {
                "user_id": 1,
                "name": "Grant Seward",
                "email": "grant@treeschema.com"
            }
        }
        resp_obj = {"data_field": resp_data_field}
        
        response = requests.Response()
        response.status_code = 201
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.update_field(
            data_store_id=1,
            data_schema_id=1,
            field_id=1,
            field_updates={'data_schema': 'inputs'}
        )
        assert resp == resp_obj


    @patch('treeschema.api.client.r.post')  
    def test_add_tag_to_data_field(self, mock_get):
        tags = ['new_tag', 'second tag']
        resp_statuses = ['added', 'exists']

        resp_data_store = {
            "tags": tags,
            "tag_statuses": resp_statuses
        }

        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_data_store
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.add_tag_to_field(
            data_store_id=1,
            data_schema_id=1,
            field_id=1,
            tags=tags
        )
        assert resp == resp_data_store

    @patch('treeschema.api.client.r.delete')  
    def test_remove_tag_from_field(self, mock_get):
        tags = ['new_tag', 'second tag']
        resp_data_field = {
            "tags_removed": tags
        }

        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_data_field
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.remove_field_tags(
            data_store_id=1,
            data_schema_id=1,
            field_id=1,
            tags=tags
        )
        assert resp == resp_data_field

    @patch('treeschema.api.client.r.get')  
    def test_get_get_all_values_for_field(self, mock_get):
        resp_field_values = [
            {
                "field_value_id": 396,
                "field_value": "01",
                "description_markup": "<p>New customer</p>",
                "description_raw": "New customer",
                "created_ts": "2020-08-15 22:10:18",
                "updated_ts": "2020-08-15 22:10:18"
            },
            {
                "field_value_id": 2,
                "field_value": "02",
                "description_markup": None,
                "description_raw": None,
                "created_ts": "2020-08-15 22:10:18",
                "updated_ts": "2020-08-15 22:10:18"
            }
        ]
        resp_obj = {
            "meta": {
                "current_page": 1,
                "next_page": None,
                "total_cnt": 1
            },
            "field_values": resp_field_values
        }

        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_all_values_for_field(
            data_store_id=1,
            data_schema_id=1,
            field_id=1
        )
        assert resp == resp_field_values

    @patch('treeschema.api.client.r.get')  
    def test_get_field_value_by_name(self, mock_get):
        resp_field_value = {
            "field_value_id": 2,
            "field_value": "01",
            "description_markup": None,
            "description_raw": None,
            "created_ts": "2020-08-15 22:10:18",
            "updated_ts": "2020-08-15 22:10:18"
        }
        resp_obj = {"field_value": resp_field_value}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_field_value_by_name(
            data_store_id=1,
            data_schema_id=1,
            field_id=1,
            value='FIRST_NAME'
        )
        assert resp == resp_obj

    @patch('treeschema.api.client.r.get')  
    def test_get_field_value_by_id(self, mock_get):
        resp_field_value = {
            "field_value_id": 2,
            "field_value": "01",
            "description_markup": None,
            "description_raw": None,
            "created_ts": "2020-08-15 22:10:18",
            "updated_ts": "2020-08-15 22:10:18"
        }
        resp_obj = {"field_value": resp_field_value}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_field_value_by_id(
            data_store_id=1,
            data_schema_id=1,
            field_id=1,
            field_value_id=1
        )
        assert resp == resp_obj

    @patch('treeschema.api.client.r.post')  
    def test_create_field_value(self, mock_get):
        resp_field_value = {
            "field_value_id": 2,
            "field_value": "01",
            "description_markup": None,
            "description_raw": None,
            "created_ts": "2020-08-15 22:10:18",
            "updated_ts": "2020-08-15 22:10:18"
        }
        resp_obj = {"field_value": resp_field_value}
        
        response = requests.Response()
        response.status_code = 201
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.create_field_value(
            data_store_id=1,
            data_schema_id=1,
            field_id=1,
            field_value_info={'data_schema': 'inputs'}
        )
        assert resp == resp_obj

    @patch('treeschema.api.client.r.post')  
    def test_update_field_value(self, mock_get):
        resp_data_field = {
            "field_value_id": 2,
            "field_value": "01",
            "description_markup": None,
            "description_raw": None,
            "created_ts": "2020-08-15 22:10:18",
            "updated_ts": "2020-08-15 22:10:18"
        }
        resp_obj = {"field_value": resp_data_field}
        
        response = requests.Response()
        response.status_code = 201
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.update_field_value(
            data_store_id=1,
            data_schema_id=1,
            field_id=1,
            field_value_id=1,
            field_value_updates={'data_schema': 'inputs'}
        )
        assert resp == resp_obj


    @patch('treeschema.api.client.r.get')  
    def test_get_all_transformations(self, mock_get):
        resp_transformations = [
            {
                "transformation_id": 25,
                "name": "My Tansform",
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
        
        client = APIClient()
        resp = client.get_all_transformations()
        assert resp == resp_transformations

    @patch('treeschema.api.client.r.get')  
    def test_transformation_by_name(self, mock_get):
        resp_transformation = {
            "transformation_id": 25,
            "name": "My Tansform",
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
        resp_obj = {"transformation": resp_transformation}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_transformation_by_name('My Tansform')
        assert resp == resp_obj

    @patch('treeschema.api.client.r.get')  
    def test_transformation_by_id(self, mock_get):
        resp_transformation = {
            "transformation_id": 25,
            "name": "My Tansform",
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
        resp_obj = {"transformation": resp_transformation}
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_transformation_by_id(1)
        assert resp == resp_obj

    @patch('treeschema.api.client.r.post')  
    def test_create_transformation(self, mock_get):
        resp_transformation = {
            "transformation_id": 25,
            "name": "My Tansform",
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
        resp_obj = {"transformation": resp_transformation}
        
        response = requests.Response()
        response.status_code = 201
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.create_transformation({'transformation': 'inputs'})
        assert resp == resp_obj


    @patch('treeschema.api.client.r.get')  
    def test_get_all_transformation_links(self, mock_get):
        resp_transformation_links = [
            {
                "transformation_link_id": 1,
                "created_ts": "2020-09-22 23:54:26",
                "updated_ts": "2020-09-22 23:54:26",
                "source_data_store_id": 3,
                "source_data_store_name": "Kafka Prod",
                "source_schema_id": 17,
                "source_schema_name": "users-topic.v1",
                "source_field_id": 200,
                "source_field_name": "user_id",
                "target_data_store_id": 4,
                "target_data_store_name": "Redshift",
                "target_schema_id": 469,
                "target_schema_name": "usr.user_info",
                "target_field_id": 5399,
                "target_field_name": "user_id"
            }
        ]
        resp_obj = {
            "meta": {
                "current_page": 1,
                "next_page": None,
                "total_cnt": 2
            },
            "transformation_links": resp_transformation_links
        }
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.get_all_transformation_links(
            transformation_id=1
        )
        assert resp == resp_transformation_links

    @patch('treeschema.api.client.r.post')  
    def test_get_transformation_link_by_id(self, mock_get):
        resp_transformation_links = {
            "links": [
                {
                    "source_field_id": 89,
                    "target_field_id": 5399
                }
            ],
            "link_statuses": [
                "exists"
            ],
            "updated_links": [
                {
                "transformation_link_id": 205,
                "source_field_id": 89,
                "source_field_name": "account_type",
                "source_schema_id": 8,
                "source_schema_name": "public.accounts",
                "source_data_store_id": 3,
                "source_data_store_name": "Postgres Prod",
                "target_field_id": 5399,
                "target_field_name": "acct_type",
                "target_schema_id": 469,
                "target_schema_name": "acct.dvc.raw.01",
                "target_data_store_id": 4,
                "target_data_store_name": "Kafka"
                }
            ]
        }
        
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = resp_transformation_links
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.create_transformation_links(
            transformation_id=1,
            links={'transformation': 'links'},
            set_state=False
        )
        assert resp == resp_transformation_links

    @patch('treeschema.api.client.r.post')  
    def test_create_transformation_link(self, mock_get):
        resp_data_store = {
            "data_store_id": 18,
            "name": "Kafka Prod Cluster",
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
        resp_obj = {"data_store": resp_data_store}
        
        response = requests.Response()
        response.status_code = 201
        response.json = MagicMock()
        response.json.return_value = resp_obj
        mock_get.return_value = response
        
        client = APIClient()
        resp = client.create_data_store({'data_store': 'inputs'})
        assert resp == resp_obj