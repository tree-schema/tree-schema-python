import requests
import unittest
from unittest.mock import MagicMock, patch

import mock
import pytest

import treeschema
from treeschema.api import APIClient
from treeschema.catalog import TreeSchemaUser


class TestBaseSerializer(unittest.TestCase):

    user_inputs = {
        'user_id': 1,
        'email': 'test@treeschema.com',
        'name': 'Test User'
    }

    def test_user_obj(self):
        usr = TreeSchemaUser(self.user_inputs)
        assert usr.obj == self.user_inputs


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
