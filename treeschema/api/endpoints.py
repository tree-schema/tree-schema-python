import os

__DEFAULT_HOST__ = 'https://api.treeschema.com'

# API Endpoints for Tree Schema 

TREE_SCHEMA_BASE_URL = os.environ.get('TREE_SCHEMA_HOST', __DEFAULT_HOST__)

# Users 
USERS                = TREE_SCHEMA_BASE_URL + '/catalog/users'
USER                 = TREE_SCHEMA_BASE_URL + '/catalog/users/{user_id}'

# Data Stores
DATA_STORES          = TREE_SCHEMA_BASE_URL + '/catalog/data-stores'
DATA_STORE           = TREE_SCHEMA_BASE_URL + '/catalog/data-stores/{data_store_id}'
DATA_STORE_TAGS      = TREE_SCHEMA_BASE_URL + '/catalog/data-stores/{data_store_id}/tags'

# Schemas 
SCHEMAS              = DATA_STORE + '/schemas'
SCHEMA               = DATA_STORE + '/schemas/{data_schema_id}'
SCHEMA_TAGS          = DATA_STORE + '/schemas/{data_schema_id}/tags'

# Fields
FIELDS               = SCHEMA + '/fields'
FIELD                = SCHEMA + '/fields/{field_id}'
FIELD_TAGS           = SCHEMA + '/fields/{field_id}/tags'

# Field Values
FIELD_VALUES         = FIELD + '/values'
FIELD_VALUE          = FIELD + '/values/{field_value_id}'

# Transformations
TRANSFORMATIONS      = TREE_SCHEMA_BASE_URL + '/catalog/transformations'
TRANSFORMATION       = TREE_SCHEMA_BASE_URL + '/catalog/transformations/{transformation_id}'
TRANSFORMATION_TAGS  = TREE_SCHEMA_BASE_URL + '/catalog/transformations/{transformation_id}/tags'

# Transformation links
TRANSFORMATION_LINKS = TRANSFORMATION + '/links'
TRANSFORMATION_LINK  = TRANSFORMATION + '/links/{transformation_link_id}'

