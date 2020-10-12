import requests as r
from typing import Any, Dict, List

from . import endpoints
from ..exceptions import TreeSchemaApiError
from .. import TreeSchemaAuth


class APIClient(object):
    """The underlying client that manages all interactions
    with the Tree Schema REST API.
    """
    __APPL_JSON_HDR__ = {'Content-Type': 'application/json'}

    def __init__(self, *args, **kwargs) -> None: 
        self.auth = TreeSchemaAuth()
        self.base_headers = {'Authorization': 'Basic ' + self.auth.encoded_secret}

    def _get_by_url(self, url: str, params: Dict[str, Any] = None) -> Dict:
        """Executes a GET from the Tree Schema URL and returns the results

        :param url: The URL for the API
        :returns: A dictionary of response values, this can be null or contain
                  error messages if the entity is not found
        """
        get_inputs = {'headers': self.base_headers}
        if params:
            get_inputs['params'] = params

        resp = r.get(url, **get_inputs)
        if resp.status_code >= 400:
            raise TreeSchemaApiError(
                'Error: %s' % resp.text
            )
        return resp.json()

    def _get_paginated_by_url(self, url: str, pagininate_resp_key: str) -> List[Dict]:
        """Gets all objects that exist from a paginated API
        
        :param url: the endpoint to query
        :param pagininate_resp_key: the response key that contains the list of items
        """
        entities = []
        has_next = True
        params = {'page': 1}
        while has_next:
            found = self._get_by_url(url, params=params)
            entities.extend(found[pagininate_resp_key])

            if 'meta' in found.keys():
                next_page = found['meta']['next_page']
                if next_page is None:
                    has_next = False
                else:
                    params['page'] += 1 
            
        return entities

    def _post_to_url(self, url: str, json_body: Dict, params: Dict = None) -> Dict:
        """Sends a post request to Tree Schema 

        :param url: The URL for the API
        :param json_body: A dictionary of values to send 
        :returns: A dictionary response from the request
        """
        inputs = {'json': json_body, 'headers': self.base_headers}
        if params:
            inputs['params'] = params
        resp = r.post(url, **inputs)
        if resp.status_code >= 400:
            raise TreeSchemaApiError(
                'Error: %s' % resp.text
            )
        return resp.json()

    def _delete_by_url(self, url: str, json_body: Dict = None) -> bool:
        """Executes a DELETE from the Tree Schema URL 

        :param url: The URL for the API
        :returns: A boolean, True if the delete is successful
        """
        body = {}
        if json_body:
            body['json'] = json_body

        resp = r.delete(url, headers=self.base_headers, **body)
        if resp.status_code >= 400:
            success = False
        else:
            success = True 
        return success

    def get_all_users(self) -> Dict: 
        """Retrieves all users in the user's organization"""
        url = endpoints.USERS
        return self._get_paginated_by_url(
            url,
            pagininate_resp_key='users'
        )
    
    def get_user_by_email(self, email: str):
        """Retrieves a user from their email"""
        url = endpoints.USERS
        params = {'email': email}
        return self._get_by_url(
            url,
            params=params
        )


    def get_user(self, user_id: int, email: str = None) -> Dict: 
        """Retrieves a user"""
        args = {'user_id': user_id}
        url = endpoints.USER.format(**args)
        
        params = {}
        if email:
            params['email'] = email
        return self._get_by_url(
            url,
            **params
        )

    def get_all_data_stores(self) -> Dict: 
        """Retrieves a data store from the Tree Schema API"""
        url = endpoints.DATA_STORES
        return self._get_paginated_by_url(
            url,
            pagininate_resp_key='data_stores'
        )

    def get_data_store_by_name(self, name) -> Dict[str, Any]:
        """Retrieves a data store from the Tree Schema API"""
        url = endpoints.DATA_STORES
        return self._get_by_url(
            url,
            params={'name': name}
        )

    def get_data_store_by_id(self, data_store_id) -> Dict[str, Any]:
        """Retrieves a data store from the Tree Schema API"""
        args = {'data_store_id': data_store_id}
        url = endpoints.DATA_STORE.format(**args)
        return self._get_by_url(url)

    def create_data_store(self, data_store_info) -> Dict:
        """Creates a data store via the Tree Schema API"""
        url = endpoints.DATA_STORES
        return self._post_to_url(
            url, 
            json_body=data_store_info
        )

    def add_tag_to_data_store(self, data_store_id, tags) -> bool:
        """Adds tags to a data store via the Tree Schema API"""
        _tags = {'tags': tags}
        args = {'data_store_id': data_store_id}
        url = endpoints.DATA_STORE_TAGS.format(**args)
        return self._post_to_url(
            url, 
            json_body=_tags,
        )

    def get_all_schemas_for_data_store(self, data_store_id) -> List[Dict]:
        args = {'data_store_id': data_store_id}
        url = endpoints.SCHEMAS.format(**args)
        data_schemas = self._get_paginated_by_url(
            url,
            pagininate_resp_key='data_schemas'
        )            
        return data_schemas

    def get_data_schema_by_name(
        self, 
        data_store_id: int, 
        name: str
    ) -> Dict[str, Any]:
        """Retrieves a single schema from a data store via the Tree Schema API"""
        args = {'data_store_id': data_store_id}
        url = endpoints.SCHEMAS.format(**args)
        return self._get_by_url(
            url,
            params={'name': name}
        )
        
    def get_data_schema_by_id(
        self, 
        data_store_id: int, 
        data_schema_id: int
    ) -> Dict[str, Any]:
        """Retrieves a single schema from a data store via the Tree Schema API"""
        args = {'data_store_id': data_store_id, 'data_schema_id': data_schema_id}
        url = endpoints.SCHEMA.format(**args)
        return self._get_by_url(url)

    def create_data_schema(self, data_store_id, data_schema_info) -> bool:
        """Creates a data schema via the Tree Schema API"""
        args = {'data_store_id': data_store_id}
        url = endpoints.SCHEMAS.format(**args)
        return self._post_to_url(
            url, 
            json_body=data_schema_info
        )

    def add_tag_to_data_schema(self, data_store_id, data_schema_id, tags) -> bool:
        """Adds tags to a data schema via the Tree Schema API"""
        _tags = {'tags': tags}
        args = {'data_store_id': data_store_id, 'data_schema_id': data_schema_id}
        url = endpoints.SCHEMA_TAGS.format(**args)
        return self._post_to_url(
            url, 
            json_body=_tags
        )

    def delete_schemas_from_data_store(self, data_store_id, delete_schemas) -> bool:
        """Deletes (deprecates) a list of schemas from a data store 
        via the Tree Schema API
        """
        args = {'data_store_id': data_store_id}
        url = endpoints.SCHEMAS.format(**args)
        return self._delete_by_url(url, json_body=delete_schemas)

    def get_all_fields_for_schema(
        self, 
        data_store_id: int, 
        data_schema_id: int
    ) -> List[Dict]:
        args = {'data_store_id': data_store_id, 'data_schema_id': data_schema_id}
        url = endpoints.FIELDS.format(**args)
        fields = self._get_paginated_by_url(
            url,
            pagininate_resp_key='data_fields'
        )            
        return fields

    def get_data_field_by_name(
        self,
        data_store_id: int,
        data_schema_id: int,
        name: str
    ) -> Dict[str, Any]:
        """Retrieves a single schema from a data store via the Tree Schema API"""
        args = {'data_store_id': data_store_id, 'data_schema_id': data_schema_id}
        url = endpoints.FIELDS.format(**args)
        return self._get_by_url(
            url,
            params={'name': name}
        )

    def get_data_field_by_id(
        self, 
        data_store_id: int, 
        data_schema_id: int,
        field_id: int
    ) -> Dict[str, Any]:
        args = {
            'data_store_id': data_store_id, 
            'data_schema_id': data_schema_id, 
            'field_id': field_id
        }
        url = endpoints.FIELD.format(**args)
        return self._get_by_url(url)

    def create_data_field(
        self, 
        data_store_id: int, 
        data_schema_id: int,
        data_field_info
    ) -> bool:
        args = {'data_store_id': data_store_id, 'data_schema_id': data_schema_id}
        url = endpoints.FIELDS.format(**args)
        return self._post_to_url(
            url, 
            json_body=data_field_info
        )

    def update_field(
        self, 
        data_store_id: int, 
        data_schema_id: int,
        field_id: int,
        field_updates: dict
    ) -> Dict:
        args = {
            'data_store_id': data_store_id, 
            'data_schema_id': data_schema_id, 
            'field_id': field_id
        }
        url = endpoints.FIELD.format(**args)
        return self._post_to_url(
            url, 
            json_body=field_updates
        )

    def add_tag_to_field(self, data_store_id, data_schema_id, field_id, tags) -> bool:
        """Adds tags to a data schema via the Tree Schema API"""
        _tags = {'tags': tags}
        args = {
            'data_store_id': data_store_id, 
            'data_schema_id': data_schema_id, 
            'field_id': field_id
        }
        url = endpoints.FIELD_TAGS.format(**args)
        return self._post_to_url(
            url, 
            json_body=_tags
        )

    def delete_fields_from_schema(self, data_store_id, data_schema_id, delete_fields) -> bool:
        """Deletes (deprecates) a list of schemas from a data store 
        via the Tree Schema API
        """
        args = {'data_store_id': data_store_id, 'data_schema_id': data_schema_id}
        url = endpoints.FIELDS.format(**args)
        return self._delete_by_url(url, json_body=delete_fields)

    def get_all_values_for_field(self, data_store_id, data_schema_id, field_id) -> List[Dict]:
        """Retrieves all of the sample values for a given field"""
        args = {
            'data_store_id': data_store_id, 
            'data_schema_id': data_schema_id, 
            'field_id': field_id
        }
        url = endpoints.FIELD_VALUES.format(**args)
        field_values = self._get_paginated_by_url(
            url,
            pagininate_resp_key='field_values'
        )            
        return field_values

    def get_field_value_by_name(
        self,
        data_store_id: int,
        data_schema_id: int,
        field_id: int,
        value: str
    ) -> Dict[str, Any]:
        """Retrieves a single schema from a data store via the Tree Schema API"""
        args = {
            'data_store_id': data_store_id, 
            'data_schema_id': data_schema_id,
            'field_id': field_id
        }
        url = endpoints.FIELD_VALUES.format(**args)
        return self._get_by_url(
            url,
            params={'value': value}
        )


    def get_field_value_by_id(
        self, 
        data_store_id: int,
        data_schema_id: int, 
        field_id: int,
        field_value_id: int
    ) -> Dict[str, Any]:
        """Rertieves a field value via the Tree Schema API"""
        args = {
            'data_store_id': data_store_id, 
            'data_schema_id': data_schema_id, 
            'field_id': field_id,
            'field_value_id': field_value_id
        }
        url = endpoints.FIELD_VALUE.format(**args)
        return self._get_by_url(url)
    
    def create_field_value(
        self, 
        data_store_id: int, 
        data_schema_id: int,
        field_id: int,
        field_value_info: dict
    ) -> bool:
        args = {
            'data_store_id': data_store_id, 
            'data_schema_id': data_schema_id, 
            'field_id': field_id
        }
        url = endpoints.FIELD_VALUES.format(**args)
        return self._post_to_url(
            url, 
            json_body=field_value_info
        )

    def update_field_value(
        self, 
        data_store_id: int, 
        data_schema_id: int,
        field_id: int,
        field_value_id: int,
        field_value_updates: dict
    ) -> Dict:
        args = {
            'data_store_id': data_store_id, 
            'data_schema_id': data_schema_id, 
            'field_id': field_id,
            'field_value_id': field_value_id
        }
        url = endpoints.FIELD_VALUE.format(**args)
        return self._post_to_url(
            url, 
            json_body=field_value_updates
        )

    def get_all_transformations(self) -> Dict: 
        """Retrieves a data store from the Tree Schema API"""
        url = endpoints.TRANSFORMATIONS
        return self._get_paginated_by_url(
            url,
            pagininate_resp_key='transformations'
        )

    def get_transformation_by_name(self, name: str) -> Dict[str, Any]:
        """Retrieves a transformation via the Tree Schema API"""
        url = endpoints.TRANSFORMATIONS
        return self._get_by_url(
            url,
            params={'name': name}
        )

    def get_transformation_by_id(self, transformation_id) -> Dict[str, Any]:
        """Retrieves a data store from the Tree Schema API"""
        args = {'transformation_id': transformation_id}
        url = endpoints.TRANSFORMATION.format(**args)
        return self._get_by_url(url)

    def create_transformation(self, transformation_info) -> Dict:
        """Creates a data store via the Tree Schema API"""
        url = endpoints.TRANSFORMATIONS
        return self._post_to_url(
            url, 
            json_body=transformation_info
        )

    def get_all_transformation_links(self, transformation_id) -> Dict:
        """Creates a data store via the Tree Schema API"""
        args = {'transformation_id': transformation_id}
        url = endpoints.TRANSFORMATION_LINKS.format(**args)
        transformation_links = self._get_paginated_by_url(
            url,
            pagininate_resp_key='transformation_links'
        )            
        return transformation_links
    
    def create_transformation_links(
        self, 
        transformation_id, 
        links, 
        set_state
    ) -> Dict:
        """Gets or creates a list of links via the Tree Schema API.
        Optionally will set the state of the transformaiton such 
        that the transformation contains exactly the links that 
        were provided via the API
        """
        args = {'transformation_id': transformation_id}
        if set_state:
            params = {'set_state': True}
        else:
            params = {}
        url = endpoints.TRANSFORMATION_LINKS.format(**args)
        return self._post_to_url(
            url, 
            json_body=links,
            params=params
        )

    def delete_links_from_transformation(self, transformation_id, delete_links) -> bool:
        """Deletes (deprecates) a list of schemas from a data store 
        via the Tree Schema API
        """
        args = {'transformation_id': transformation_id}
        url = endpoints.TRANSFORMATION_LINKS.format(**args)
        return self._delete_by_url(url, json_body=delete_links)

    def get_transformation_link_by_id(
        self, 
        transformation_id, 
        transformation_link_id
    ) -> Dict:
        """Deletes (deprecates) a list of schemas from a data store 
        via the Tree Schema API
        """
        args = {
            'transformation_id': transformation_id,
            'transformation_link_id': transformation_link_id
        }
        url = endpoints.TRANSFORMATION_LINK.format(**args)
        return self._get_by_url(url)
    
