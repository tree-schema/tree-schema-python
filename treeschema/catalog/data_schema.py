from typing import Any, Dict, List

from . import DataField, TreeSchemaSerializer, TreeSchemaUser
from .tags import get_tags_added
from ..exceptions import DataAssetDoesNotExist


class DataSchema(TreeSchemaSerializer):
    """An object that represents a single data schema."""
    __ID_FIELD_NAME__ = 'data_schema_id'
    __NAME_FIELD__ = 'name'
    __FIELDS__ = {
        'created_ts': str,
        'data_schema_id': int,
        'description_markup': str,
        'description_raw': str,
        'name': str,
        'schema_loc': str,
        'steward': lambda x: TreeSchemaUser(x),
        'tech_poc': lambda x: TreeSchemaUser(x),
        'type': str,
        'updated_ts': str
    }

    def __init__(
        self,
        data_schema_inputs: [int, str, Dict],
        data_store_id: int,
        *args, 
        **kwargs
    ):
        """Create a data schema object with either the ID of a data schema,
        the name of the data schema, or the fully defined data schema object 
        as a dictionary.

        :param data_schema_inputs: the inputs to create or retrieve 
            the data schema
        :param data_store_id: The ID of the data store that this schema
            belongs to
        """
        self.data_store_id = data_store_id
        self.tags = []
        self._fields_by_id = {}
        self._fields_by_name = {}
        self._fields_retrieved = False
        super(DataSchema, self).__init__(data_schema_inputs)

    def _get_self_by_id(self):
        raw_resp = self.client.get_data_schema_by_id(
            data_store_id=self.data_store_id, data_schema_id=self.id
        )
        return raw_resp.get('data_schema')

    def _get_self_by_name(self):
        raw_resp = self.client.get_data_schema_by_name(
            data_store_id=self.data_store_id, name=self._name
        )
        return raw_resp.get('data_schema')
    
    def add_tags(self, tags: List[str]) -> Dict:
        """Adds one or more tags to the data schema

        :param tags: a list of tags, a single tag can also be passed
        :returns: the API response

        >>> my_schema = ts.data_store('my data store').schema('some schema')
        >>> my_schema.add_tags(['new_tag', 'a second new tag'])
        >>> my_schema.add_tags('single tag')
        """
        if not isinstance(tags, list):
            tags = [tags]

        resp = None
        tags_to_add = [t for t in tags if t not in self.tags]
        if len(tags_to_add) > 0:
            tag_res = self.client.add_tag_to_data_schema(
                data_store_id=self.data_store_id, 
                data_schema_id=self.id, 
                tags=tags_to_add
            )
            added_tags = get_tags_added(tag_res)
            self.tags.extend(added_tags)
            resp = tag_res
        return resp
        
    def _create(self):
        data_schema = {}
        if not self._is_validated:
            data_schema_raw = self.client.create_data_schema(
                self.data_store_id, 
                self._raw_inputs
            )
            data_schema = data_schema_raw.get('data_schema')
            if data_schema:
                self._is_validated = True
                self.id = data_schema['data_schema_id']
        return data_schema

    @property
    def fields(self):
        self._check_retrieve_fields()
        return self._fields_by_id

    def _add_data_field(self, data_field: DataField) -> None:
        """Adds a data schema to the internal mappings"""
        self._fields_by_id[data_field.id] = data_field
        self._fields_by_name[data_field.name.lower()] = data_field

    def _remove_data_field(self, field_id: int) -> None:
        """Removes a schema from the internal mappings"""
        field = self._fields_by_id.pop(field_id, None)
        if field:
            self._fields_by_name.pop(field.name.lower(), None)

    def _reset_data_fields(self) -> None:
        """Resets all field value mappings"""
        self._fields_by_id = {}
        self._fields_by_name = {}

    def _check_retrieve_fields(self, force_refresh=False):
        if not self._fields_retrieved or force_refresh: 
            self.get_fields()

    def get_fields(self, refresh: bool =False) -> List[DataField]:
        """Retrieves all fields from the data schema. After this is called
        for the first time the fields are cached locally.

        :param refresh: Default False, if True, will force all fields 
            to be retrieved from Tree Schema and not the local cache
        :returns: a list of `DataField` objects that belong to this schema
        """
        if refresh or not self._fields_retrieved:
            if refresh:
                self._reset_data_fields()
            field_results = self.client.get_all_fields_for_schema(
                data_store_id=self.data_store_id,
                data_schema_id=self.id
            )
            self._fields_retrieved = True
            for field in field_results:
                found_field = DataField(
                    field, 
                    data_store_id=self.data_store_id,
                    data_schema_id=self.id
                )
                self._add_data_field(found_field)

        return self.fields

    def field(self, 
        field_inputs: [int, Dict], 
        refresh: bool = False,
        raise_if_not_exist: bool = False
    ):
        """Creates or retrieves a field object, Inputs can be an integer 
        (for the field ID), a string (for the field name), 
        or a dictionary of values used to create the field

        :param field_inputs: the inputs used to create or retrieve
            the field
        :param refresh: whether or not to force a refresh from the database,
            the default is False
        :param raise_if_not_exist: default is False, if True will raise a 
            `treescheam.exceptions.DataAssetDoesNotExist` exception if the field does 
            not exists, when False `None` is returned for fields that do not exist

        :returns: a Data Field object

        >>> my_schema = ts.data_store('my data store').schema('some schema')
        >>> field_1 = my_schema.field(1)
        >>> field_2 = my_schema.field('second_field')
        >>> field_inputs = {
        >>>     'name': 'new_field', 'type': 'scalar', 'data_type': 'number',
        >>>     'data_format': 'bigint', 'description': 'My python description'
        >>> }
        >>> field_3 = my_schema.field(field_inputs)

        It is possible to create a data field by passing in a native python type
        for the `type`, `data_type` and `data_format` inputs, however, only the 
        `type` field is required. For example, a field can be created as:

        >>> field_inputs = {
        >>>     'name': 'new_field', 'type': str, 'data_type': str, 'data_format': str
        >>> }
        >>> my_schema.field(field_inputs)

        Or as little as just the name and type

        >>> my_schema.field({'name': 'new_field', 'type': float})

        The fields inputs managed by the API, all required fields for data 
        fields REST can be found in BODY of the the API to 
        `Create a Field <https://developer.treeschema.com/rest-api/#create-a-field>`_ 
        this Python client only requires `name` and `type` IF the type is a native 
        Python type (e.g. `str`, `float`, `int`, `bool`, `bytes`, `list` or `dict`) 
        it will try to infer the values of the remaining fields from these native types.

        """
        # Pre-fetch all fields for the schema on the first retrival
        self._check_retrieve_fields(refresh)

        field = None
        if (isinstance(field_inputs, int) 
            and field_inputs in self._fields_by_id):
            field = self._fields_by_id[field_inputs]
        elif (isinstance(field_inputs, str) 
            and field_inputs.lower() in self._fields_by_name):
            field = self._fields_by_name[field_inputs.lower()]
        elif isinstance(field_inputs, dict):
            field = DataField(
                field_inputs, 
                data_store_id=self.data_store_id,
                data_schema_id=self.id
            )
            self._add_data_field(field)
        
        if raise_if_not_exist and not field:
            raise DataAssetDoesNotExist('The field requested: %s does not exist' % field_inputs) 

        return field

    def delete_fields(
        self, 
        remove_fields: [List[int], int, List[DataField], DataField]
    ) -> bool:
        """Deletes (deprecates) a single field or list of field from
        the data schema.

        :param remove_fields: The fields to remove, can be passed 
            as the field ID or a `DataField` object. Values being passed
            can be a single field or a list of fields
        :returns: True if the fields are deprecated

        >>> my_schema = ts.data_store('my data store').schema('some schema')
        >>> delete_field = my_schema.field('some_field')
        >>> my_schema.delete_fields(delete_field)
        True
        """
        if not isinstance(remove_fields, list):
            remove_fields = [remove_fields]

        _scalar_fields = [self._scalar_or_entity_id(i) for i in remove_fields]
        delete_fields = {'field_ids': _scalar_fields}
        deleted = self.client.delete_fields_from_schema(
            data_store_id=self.data_store_id, 
            data_schema_id=self.id, 
            delete_fields=delete_fields
        )
        if deleted:
            for fid in _scalar_fields:
                self._remove_data_field(fid)
        return deleted
