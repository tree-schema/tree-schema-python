
from typing import Dict, List

from . import TreeSchemaAuth
from .api import APIClient
from .catalog import DataStore, Transformation, TreeSchemaUser
from .exceptions import InvalidInputs, UsernameSecretRequired
from .ts_enums import FIELD, SCHEMA, DATA_STORE


class TreeSchema(object):
    """The base object used to interact with your TreeSchema
    data catalog.

    The `TreeSchema` object has entry points into retrieving 
    or creating the users, Transformations and Data Stores. All 
    other data assets can be retrieved from there.

    You can use your email and Tree Schema secret key to instantiate 
    a `TreeSchema` object

    >>> from treeschema import TreeSchema
    >>> ts = TreeSchema('<your email>', '<your secret key>')
    """
    def __new__(
        cls, 
        username: str = None,
        secret_key: str = None,
        *args, 
        **kwargs
    ):
        """Creates a singleton for the Tree Schema auth object. This 
        allows the auth credentails to be passed in one time and to
        allow multiple clients to reuse the credentials without having 
        the context of the username and secret.

        :param username: The username in Tree Schema
        :param secret_key: The secret key for the username in Tree Schema
        """
        if not hasattr(cls, 'instance'):
            if not username or not secret_key:
                _msg = (
                    """Must provided username and API secret key, make 
                    sure to create the 'TreeSchema' class first
                    """
                )
                raise UsernameSecretRequired(_msg)
            cls.username = username
            cls.auth = TreeSchemaAuth(username, secret_key)
            cls.client = APIClient()
            cls._entity_holder = _EntityHolder()
            cls._data_stores_retrieved = False
            cls._transformations_retrieved = False
            cls._users_retrieved = False
            cls.instance = super(TreeSchema, cls).__new__(cls)
            # cls.encoded_secret = get_encoded_secret(username, secret_key)
        return cls.instance

    @property
    def data_stores(self):
        """A dictionary of data stores that have been 
        fetched via the API. Note - this will not contain
        all of your data stores unless you explicitly 
        fetch them with `get_data_stores()` first
        """
        return self._entity_holder._data_stores_by_id

    @property
    def transformations(self):
        """A dictionary of transformations that have been 
        fetched via the API. Note - this will not contain
        all of your transformations unless you explicitly 
        fetch them with `get_transformations()` first
        """
        return self._entity_holder._transformations_by_id

    @property
    def users(self):
        """A dictionary of users that have been 
        fetched via the API. Note - this will not contain
        all of your users unless you explicitly 
        fetch them with `get_users()` first
        """
        return self._entity_holder._users_by_id

    def _add_data_store(self, data_store):
        """Adds a data store to the internal mappings"""
        self._entity_holder._data_stores_by_id[data_store.id] = data_store
        self._entity_holder._data_stores_by_name[data_store._name.lower()] = data_store        

    def _add_transformation(self, transformation):
        """Adds a transformation to the internal mappings"""
        self._entity_holder._transformations_by_id[transformation.id] = transformation
        self._entity_holder._transformations_by_name[transformation._name.lower()] = transformation        

    def _add_user(self, user):
        """Adds a user to the internal mappings"""
        self._entity_holder._users_by_id[user.id] = user
        self._entity_holder._users_by_email[user._name.lower()] = user 

    def data_store(
        self,
        data_store_input: [int, str, Dict]
    ) -> DataStore:
        """Gets or creates a data store. A data store can be 
        retrieved by passing in the data store ID (an integer) or
        the name of the data store (a string). A data store can 
        be created using this function by passing in a dicitonary
        of values that includes the required fields for a data 
        field object.
        
        If attempting to create a new data 
        store and the name provided already exists the existing
        data store will be retrieved.

        A data store can be created or retrieved by providing the 
        data store ID, the name of the data store, or a dictionary of 
        inputs which can be used to create a new data store.

        >>> ds = ts.data_store(1)
        >>> ds = ts.data_store('Data store name')
        >>> ds = ts.data_store({'name': 'New DS', 'type': 'other'})

        The required fields are managed by the API, all required fields for data 
        stores can be found in BODY of the the API to 
        `Create a Data Store <https://developer.treeschema.com/rest-api/#create-a-data-store>`_
        """
        # Pre-fetch all transformations on the first retrieval
        if not self._data_stores_retrieved: 
            self.get_data_stores()

        if (isinstance(data_store_input, int) 
            and data_store_input in self._entity_holder._data_stores_by_id):
            data_store = self._entity_holder._data_stores_by_id[data_store_input]
        elif (isinstance(data_store_input, str) 
            and data_store_input.lower() in self._entity_holder._data_stores_by_name):
            data_store = self._entity_holder._data_stores_by_name[data_store_input.lower()]
        else:
            data_store = DataStore(data_store_input)
            self._add_data_store(data_store)
        return data_store
    
    def get_data_stores(
        self, 
        refresh: bool = False
    ) -> Dict[int, DataStore]:
        """Retrieves all data stores from Tree Schema"""
        if refresh or not self._data_stores_retrieved:
            ds_results = self.client.get_all_data_stores()
            self._data_stores_retrieved = True
            for ds in ds_results:
                found_ds = DataStore(ds)
                self._add_data_store(found_ds)
            
        return self.data_stores    
    
    def transformation(
        self, 
        transformation_input: [int, str, Dict]
    ) -> DataStore:
        """Gets or creates a transformation. A transformation can be 
        retrieved by passing in the transformation ID (an integer) or
        the name of the transformation (a string). A transformation can 
        be created using this function by passing in a dicitonary
        of values that includes the required fields for a transformation
        object.

        If attempting to create a new transformation 
        and the name provided already exists the existing
        transformation will be retrieved.


        >>> t = ts.transformation(1)
        >>> t = ts.transformation('Transformation name')
        >>> t = ts.transformation({'name': 'New DS', 'type': 'pub_sub_event'})

        The required fields are managed by the API, all required fields for  
        transformations can be found in BODY of the the API to 
        `Create a Transformation <https://developer.treeschema.com/rest-api/#create-a-transformation>`_

        """
        # Pre-fetch all transformations on the first retrieval
        if not self._transformations_retrieved: 
            self.get_transformations()

        if (isinstance(transformation_input, int) 
            and transformation_input in self._entity_holder._transformations_by_id):
            transformation = self._entity_holder._transformations_by_id[transformation_input]
        elif (isinstance(transformation_input, str) 
            and transformation_input.lower() in self._entity_holder._transformations_by_name):
            transformation = self._entity_holder._transformations_by_name[transformation_input.lower()]
        else:
            transformation = Transformation(transformation_input)
            self._add_transformation(transformation)
        return transformation

    def get_transformations(
        self, 
        refresh: bool = False
    ) -> Dict[int, Transformation]:
        """Retrieves all transformations from Tree Schema"""
        if refresh or not self._transformations_retrieved:
            transform_results = self.client.get_all_transformations()
            self._transformations_retrieved = True
            for tf in transform_results:
                transformation = Transformation(tf)
                self._add_transformation(transformation)
            
        return self.transformations    

    def user(
        self, 
        user_input: [int, str]
    ) -> Dict[int, TreeSchemaUser]:
        """Retrieves a single user from the current user's org. A user can
        be looked up using the user ID or email address that the user 
        has associated to their Tree Schema account 

        >>> user = ts.user(1)
        >>> user = ts.user('user@email.com')
        
        """

        # Pre-fetch all users on the first retrieval
        if not self._users_retrieved: 
            self.get_users()

        if (isinstance(user_input, int) 
            and user_input in self._entity_holder._users_by_id):
            user = self._entity_holder._users_by_id[user_input]
        if (isinstance(user_input, str) 
            and user_input.lower() in self._entity_holder._users_by_email):
            user = self._entity_holder._users_by_email[user_input.lower()]
        else:
            user = TreeSchemaUser(user_input)
            self.users[user.id] = user
        return user

    def get_users(
        self, 
        refresh: bool = False
    ) -> Dict[int, TreeSchemaUser]:
        """Retrieves all transformations from Tree Schema"""
        if refresh or not self._users_retrieved:
            user_results = self.client.get_all_users()
            self._users_retrieved = True
            for usr in user_results:
                user = TreeSchemaUser(usr)
                self._add_user(user)
            
        return self.users

    def batch_load_by_id(
        self,
        data_store_ids: List[int] = None,
        schema_ids: List[int] = None,
        field_ids: List[int] = None,
        batch_size: int = 100
    ):
        """Batch loads a set of data assets in a more efficient manner
        by reducing the API overhead. A list of IDs can be provided for
        data stores, schemas and fields. When a list of fields is provided
        for a given assset class, all parent assets will be returned 
        as well. For example, passing in a list of field_ids will return
        the fields, their associated schemas and the schema's associated
        data stores.

        All assets are serialized and added to the TreeSchema entity holder
        as if they had been created individually. 

        >>> Example...
        """
        assets = []
        if isinstance(field_ids, list):
            assets.extend([{'type': FIELD, 'id': i} for i in field_ids])

        if isinstance(schema_ids, list):
            assets.extend([{'type': SCHEMA, 'id': i} for i in schema_ids])

        if isinstance(data_store_ids, list):
            assets.extend([{'type': DATA_STORE, 'id': i} for i in data_store_ids])

        if not assets:
            raise InvalidInputs(
                'Must provide at least one of "data_store_ids", "schema_ids" or "field_ids"'
            )
        
        has_more_assets = True
        asset_iter = 0
        while has_more_assets:
            asset_batch = assets[asset_iter * batch_size: asset_iter * batch_size + batch_size]
            if len(asset_batch) == 0:
                break

            resp = self.client.batch_retrieve_assets(assets={'assets': asset_batch})
            data_stores_found = resp.get('data_stores') or []
            schemas_found = resp.get('data_schemas') or []
            fields_found = resp.get('data_fields') or []
            for data_store in data_stores_found:
                self.data_store(data_store)

            # Keep track of schemas to data stores
            schema_ds_map = {}
            for schema in schemas_found:
                schema_ds_map[schema['data_schema_id']] = schema['data_store_id']
                self.data_store(schema['data_store_id']).schema(schema, pre_fetch=False)

            for field in fields_found:
                ds_id = schema_ds_map[field['data_schema_id']]
                schema_id = field['data_schema_id']
                self.data_store(ds_id).schema(schema_id, pre_fetch=False).field(field, pre_fetch=False)

            if len(asset_batch) < batch_size:
                has_more_assets = False

class _EntityHolder(object):
    """Holds objects to declutter the TreeSchema object"""
    def __init__(self):
        self._data_stores_by_id = {}
        self._data_stores_by_name = {}
        self._transformations_by_id = {}
        self._transformations_by_name = {}
        self._users_by_id = {}
        self._users_by_email = {}
