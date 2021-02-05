from typing import Any, Dict, List

from . import DataSchema, TreeSchemaSerializer, TreeSchemaUser
from .tags import get_tags_added, get_tags_removed
from ..exceptions import DataAssetDoesNotExist
from ..integrations.dbt import DbtManager


class DataStore(TreeSchemaSerializer):
    """An object that represents a single data store. Data stores 
    contain schemas and each data store object is able to interact 
    with the schemas that belong to it.
    """
    __ID_FIELD_NAME__ = 'data_store_id'
    __NAME_FIELD__ = 'name'
    __FIELDS__ = {
        'created_ts': str,
        'data_store_id': int,
        'description_markup': str,
        'description_raw': str,
        'details': dict,
        'name': str,
        'other_type': str,
        'steward': lambda x: TreeSchemaUser(x),
        'tech_poc': lambda x: TreeSchemaUser(x),
        'type': str,
        'updated_ts': str
    }

    def __init__(self, data_store_inputs: [int, Dict]):
        """Create a data store object with either the ID of a data store
        or the fully defined data store object as a dictionary.

        :param id: the ID for the data store
        :param inputs: a dictionary of inputs that can 
        fully serialize a data store
        """
        self._schemas_by_id = {}
        self._schemas_by_name = {}
        self._schemas_retrieved = False
        super(DataStore, self).__init__(data_store_inputs)
        self.dbt = DbtManager(self.id)
        
    def _get_self_by_id(self):
        raw_resp = self.client.get_data_store_by_id(self.id)
        return raw_resp.get('data_store')

    def _get_self_by_name(self):
        raw_resp = self.client.get_data_store_by_name(name=self._name)
        return raw_resp.get('data_store')

    @property
    def tags(self) -> List[str]:
        """Retrieves the tags for a given data store. If the tags 
        have not already been retrieved for the data store then
        the existing tags are fetched from Tree Schema 

        >>> my_data_store = ts.data_store('my data store')
        >>> my_data_store.tags
            # ['tag_1', 'tag_2']
        """
        if self._tags_fetched is False:
            tag_resp = self.client.get_data_store_tags(self.id)
            self._tags = tag_resp.get('tags', [])
            self._tags_fetched = True
        return self._tags

    def add_tags(self, tags: [str, List[str]]) -> Dict:
        """Adds one or more tags to the data store

        :param tags: a list of tags, a single tag can also be passed
        :returns: the API response

        >>> my_data_store = ts.data_store('my data store')
        >>> my_data_store.add_tags(['new_tag', 'a second new tag'])
        >>> my_data_store.add_tags('single tag')
        """
        if not isinstance(tags, list):
            tags = [tags]

        resp = None
        tags_to_add = [t for t in tags if t not in self._tags]
        if len(tags_to_add) > 0:
            tag_res = self.client.add_tag_to_data_store(self.id, tags_to_add)
            added_tags = get_tags_added(tag_res)
            self._tags.extend(added_tags)
            resp = tag_res
        return resp

    def remove_tags(self, tags: [str, List[str]]) -> Dict:
        """Removes one or more tags from the data store

        :param tags: a list of tags, a single tag can also be passed
        :returns: the API response

        >>> my_data_store = ts.data_store('my data store')
        >>> my_data_store.remove_tags(['new_tag'])
        >>> my_data_store.remove_tags('single tag')
        """
        if not isinstance(tags, list):
            tags = [tags]

        resp = None
        if len(tags) > 0:
            tag_res = self.client.remove_data_store_tags(self.id, tags)
            tags_removed = get_tags_removed(tag_res)
            if len(tags_removed) > 0:
                self._tags = [t for t in self._tags if t.lower() not in tags_removed]
            resp = tag_res
        return resp

    def _create(self):
        data_store = {}
        if not self._is_validated:
            data_store_raw = self.client.create_data_store(
                self._simplify_user_raw_inputs(self._raw_inputs)
            )
            data_store = data_store_raw.get('data_store')
            if data_store:
                self._is_validated = True
                self.id = data_store['data_store_id']
        return data_store

    @property
    def schemas(self):
        self._check_retrieve_schemas()
        return self._schemas_by_id

    def _add_data_schema(self, data_schema: DataSchema) -> None:
        """Adds a data schema to the internal mappings"""
        self._schemas_by_id[data_schema.id] = data_schema
        self._schemas_by_name[data_schema.name.lower()] = data_schema

    def _remove_data_schema(self, schema_id: int) -> None:    
        """Removes a schema from the internal mappings"""
        schema = self._schemas_by_id.pop(schema_id, None)
        if schema:
            self._schemas_by_name.pop(schema.name.lower(), None)

    def _reset_data_schemas(self) -> None:
        """Resets all field value mappings"""
        self._schemas_by_id = {}
        self._schemas_by_name = {}

    def _check_retrieve_schemas(self, force_refresh=False, pre_fetch=True):
        if (not self._schemas_retrieved and pre_fetch) or force_refresh: 
            self.get_schemas()

    def get_schemas(self, refresh: bool =False) -> List:
        """Retrieves all schemas from the data store. After this is called
        for the first time the schemas are cached locally.

        :param refresh: Default False, if True, will force all schemas 
            to be retrieved from Tree Schema and not the local cache
        :returns: a list of `DataSchema` objects that belong to this data store
        """
        if refresh or not self._schemas_retrieved:
            if refresh:
                self._reset_data_schemas()
            schema_results = self.client.get_all_schemas_for_data_store(self.id)
            self._schemas_retrieved = True
            for schema in schema_results:
                found_schema = DataSchema(schema, data_store_id=self.id)
                self._add_data_schema(found_schema)
            
        return self.schemas

    def schema(
        self, 
        schema_inputs: [int, Dict], 
        refresh: bool = False,
        pre_fetch: bool = True,
        raise_if_not_exist: bool = False
    ) -> DataSchema:
        """Creates or retrieves a `DataSchema` object, Inputs can be an integer 
        (for the schema ID), a string (for the schema name), 
        or a dictionary of values used to create the schema

        :param schema_inputs: the inputs used to create or retrieve
            the data schema
        :param refresh: whether or not to force a refresh from the database,
            the default is False
        :param pre_fetch: whether or not to pre-fetch all of the schemas for this data 
            store  during the initial load. This should primiarly be used when the inputs 
            are  a dictionary and you have already batch-retrieved the data assets required. 
            Note - you do have the option to not pre-fetch and then request a pre-fetch 
            later.
        :param raise_if_not_exist: default is False, if True will raise a 
            `treescheam.exceptions.DataAssetDoesNotExist` exception if the schema does 
            not exists, when False `None` is returned for schemas that do not exist

        :returns: a Data Schema object

        >>> my_data_store = ts.data_store('my data store')
        >>> schema_1 = my_data_store.schema(1)
        >>> schema_2 = my_data_store.schema('second.schema')
        >>> field_3 = my_data_store.field({'name': 'new_field', 'type': 'other'})
        
        The required fields are managed by the API, all required fields for data 
        schemas can be found in BODY of the the API to 
        `Create a Data Schema <https://developer.treeschema.com/rest-api/#create-a-schema>`_
        """
        # Pre-fetch all schemas for the data store on the first retrival
        self._check_retrieve_schemas(refresh, pre_fetch=pre_fetch)

        schema = None
        if (isinstance(schema_inputs, int) 
            and schema_inputs in self._schemas_by_id):
            schema = self._schemas_by_id[schema_inputs]
        elif (isinstance(schema_inputs, str) 
            and schema_inputs.lower() in self._schemas_by_name):
            schema = self._schemas_by_name[schema_inputs.lower()]
        
        if schema is None:
            schema = DataSchema(schema_inputs, data_store_id=self.id)
            self._add_data_schema(schema)

        if raise_if_not_exist and not schema:
            raise DataAssetDoesNotExist('The schema requested: %s does not exist' % schema_inputs) 

        return schema

    def delete_schemas(
        self, 
        remove_schemas: [
            int,
            List[int], 
            DataSchema,
            List[DataSchema]
        ]
    ) -> bool:
        """Deletes (deprecates) a single schema or list of schemas from
        the data store

        :param remove_schemas: a single schema or a list of schemas (these 
            can be the schema ID or a list of `DataSchema` objects)
        :returns: True if the schemas are deprecated

        >>> my_data_store = ts.data_store('my data store')
        >>> delete_schema = my_data_store.schema('some.schema')
        >>> my_data_store.delete_schemas([delete_schema])
        True
        """
        if not isinstance(remove_schemas, list):
            remove_schemas = [remove_schemas]

        _scalar_schemas = [self._scalar_or_entity_id(i) for i in remove_schemas]
        delete_schemas = {'schema_ids': _scalar_schemas}
        deleted = self.client.delete_schemas_from_data_store(
            self.id, 
            delete_schemas=delete_schemas
        )
        if deleted:
            for sid in _scalar_schemas:
                self._remove_data_schema(sid)
        return deleted


    