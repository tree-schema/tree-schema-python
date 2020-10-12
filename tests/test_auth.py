import requests
import unittest
from unittest.mock import MagicMock, patch

import mock
import pytest

from treeschema.auth import TreeSchemaAuth


class TestAuth(unittest.TestCase):
 
    def test_encoded_secret(self):
        tsa = TreeSchemaAuth()
        expected = 'dGVzdGVtYWlsQHRyZWVzY2hlbWEuY29tOjBmNGRiNTg4ZjE0MDQ2MWQ4YjJiZDllY2YzMTI5ZDEy'
        assert tsa.encoded_secret == expected
