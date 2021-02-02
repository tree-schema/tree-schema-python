import os

__DEFAULT_HOST__ = 'https://api.treeschema.com'

# API Endpoints for Tree Schema
# Allow overrides for self-hosted Tree Schema servers
TREE_SCHEMA_HOST = os.environ.get('TREE_SCHEMA_HOST', __DEFAULT_HOST__)
BASE_ENDPOINT_MAPPING = os.environ.get('BASE_ENDPOINT_MAPPING', 'catalog')

TREE_SCHEMA_BASE_URL = os.path.join(TREE_SCHEMA_HOST, BASE_ENDPOINT_MAPPING)

# Batch interactions 
BATCH_ASSETS         = TREE_SCHEMA_BASE_URL + '/batch-assets'

# Users 
USERS                = TREE_SCHEMA_BASE_URL + '/users'
USER                 = TREE_SCHEMA_BASE_URL + '/users/{user_id}'

# Data Stores
DATA_STORES          = TREE_SCHEMA_BASE_URL + '/data-stores'
DATA_STORE           = TREE_SCHEMA_BASE_URL + '/data-stores/{data_store_id}'
DATA_STORE_TAGS      = TREE_SCHEMA_BASE_URL + '/data-stores/{data_store_id}/tags'

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
TRANSFORMATIONS      = TREE_SCHEMA_BASE_URL + '/transformations'
TRANSFORMATION       = TREE_SCHEMA_BASE_URL + '/transformations/{transformation_id}'
TRANSFORMATION_TAGS  = TREE_SCHEMA_BASE_URL + '/transformations/{transformation_id}/tags'

# Transformation
TRANSFORMATION_LINKS = TRANSFORMATION + '/links'
TRANSFORMATION_LINK  = TRANSFORMATION + '/links/{transformation_link_id}'

# Transformation Breaking Changes
BREAKING_CHANGES     = TRANSFORMATION_LINKS + '/check-breaking-change'

# dbt
PARSE_MANIFEST = DATA_STORE + '/dbt/parse-manifest'
GET_MANIFEST_PARSE_RESULTS = TREE_SCHEMA_BASE_URL + '/dbt/parse-results'
SAVE_MANIFEST_PARSE_RESULTS = TREE_SCHEMA_BASE_URL + '/dbt/save-results'
