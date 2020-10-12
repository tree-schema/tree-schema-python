import pytest

from treeschema import TreeSchema

_email = 'testemail@treeschema.com'
_secret = '0f4db588f140461d8b2bd9ecf3129d12'

# Creates the basic auth object
TEST_TREE_SCHEMA = TreeSchema(_email, _secret)


