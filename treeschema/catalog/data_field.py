from typing import Any, Dict, List

from . import FieldValue, TreeSchemaSerializer, TreeSchemaUser
from .tags import get_tags_added
from ..exceptions import DataAssetDoesNotExist, InvalidFieldInputs


class DataField(TreeSchemaSerializer):
    """An object that represents a single data field."""
    __ID_FIELD_NAME__ = 'field_id'
    __NAME_FIELD__ = 'full_path_name'
    __FIELDS__ = {
        'created_ts': str,
        'data_format': str,
        'data_type': str,
        'description_markup': str,
        'description_raw': str,
        'field_id': int,
        'full_path_name': str,
        'name': str,
        'nullable': bool,
        'parent_path': str,
        'steward': lambda x: TreeSchemaUser(x),
        'tech_poc': lambda x: TreeSchemaUser(x),
        'type': str,
        'updated_ts': str
    }
    _eligible_string_types = ['scalar', 'list', 'object']
    _eligible_native_types_map = {
        str : 'string', 
        float : 'number', 
        int: 'number', 
        bytes: 'bytes',
        list: 'array',
        dict: 'object',
        bool: 'boolean'
    }


    def __init__(
        self,
        data_field_inputs: [int, str, Dict],
        data_store_id: int,
        data_schema_id: int,
        *args, 
        **kwargs
    ):
        """Create a data field object with either the ID of a data field
        or the fully defined data field object as a dictionary.

        :param data_field_inputs: the inputs to create or retrieve 
            the data field
        :param data_store_id: The ID of the data store that this field
            belongs to
        :param data_schema_id: The ID of the data schema that this field
            belongs to
        """
        self.data_store_id = data_store_id
        self.data_schema_id = data_schema_id
        self.tags = []
        self._field_values_by_id = {}
        self._field_values_by_value = {}
        self._field_values_retrieved = False
        if isinstance(data_field_inputs, dict):
            self._clean_field_inputs(data_field_inputs)
        super(DataField, self).__init__(data_field_inputs)

    def _get_self_by_id(self):
        raw_resp = self.client.get_data_field_by_id(
            data_store_id=self.data_store_id,
            data_schema_id=self.data_schema_id,
            field_id=self.id
        )
        return raw_resp.get('data_field')

    def _get_self_by_name(self):
        raw_resp = self.client.get_data_field_by_name(
            data_store_id=self.data_store_id,
            data_schema_id=self.data_schema_id,
            name=self._name
        )
        return raw_resp.get('data_field')
    
    def add_tags(self, tags: List[str]) -> Dict:
        """Adds one or more tags to the data field

        :param tags: a list of tags, a single tag can also be passed
        :returns: the API response

        >>> my_field = ts.data_store('my data store').schema('some schema').field('my_field')
        >>> my_field.add_tags(['new_tag', 'a second new tag'])
        """
        if not isinstance(tags, list):
            tags = [tags]

        resp = None
        tags_to_add = [t for t in tags if t not in self.tags]
        if len(tags_to_add) > 0:
            tag_res = self.client.add_tag_to_field(
                data_store_id=self.data_store_id, 
                data_schema_id=self.data_schema_id,
                field_id=self.id, 
                tags=tags_to_add
            )
            added_tags = get_tags_added(tag_res)
            self.tags.extend(added_tags)
            resp = tag_res
        return resp
        
    def _create(self):
        data_field = {}
        if not self._is_validated:
            data_field_raw = self.client.create_data_field(
                data_store_id=self.data_store_id, 
                data_schema_id=self.data_schema_id,
                data_field_info=self._raw_inputs
            )
            data_field = data_field_raw.get('data_field')
            if data_field:
                self._is_validated = True
                self.id = data_field['field_id']
        return data_field

    @property
    def field_values(self):
        self._check_retrieve_field_values()
        return self._field_values_by_id

    def _add_field_value(self, field_value: FieldValue) -> None:
        """Adds a field value to the internal mappings"""
        self._field_values_by_id[field_value.id] = field_value
        self._field_values_by_value[field_value.field_value.lower()] = field_value

    def _remove_field_value(self, field_value_id: int) -> None:
        """Removes a field value from the internal mappings"""
        field_value = self._field_values_by_id.pop(field_value_id, None)
        if field_value:
            self._field_values_by_value.pop(field_value.field_value.lower(), None)

    def _reset_field_values(self) -> None:
        """Resets all field value mappings"""
        self._field_values_by_id = {}
        self._field_values_by_value = {}

    def _check_retrieve_field_values(self, force_refresh=False):
        if not self._field_values_retrieved or force_refresh: 
            self.get_field_values()

    def update(self, 
        *, 
        _type: str = None,
        data_type: str = None,
        data_format: str = None,
        description: str = None,
        nullable: bool = None,
        tech_poc: [int, TreeSchemaUser] = None,
        steward: [int, TreeSchemaUser] = None
    ):
        """Update an existing field. Only keyword arguments can be provided, positional 
        arguments are not allowed.

        :param _type: the type of field, must be a Tree Schema field type
        :param data_type: the data type of field, must be JSON compatible data type
        :param data_format: a free form string that represents the data format
        :param description: The description for the field
        :param nullable: Whether or not the field can be null, `True` if yes, `False` if no
        :param tech_poc: The technical point of contact
        :param steward: The data steward
        :returns: a `DataField`, an updated version of itself
        """
        update_dict = {}
        if _type:
            update_dict['type'] = _type
        if data_type:
            update_dict['data_type'] = data_type
        if data_format:
            update_dict['data_format'] = data_format
        if description:
            update_dict['description'] = description
        if nullable:
            update_dict['nullable'] = nullable
        if tech_poc:
            update_dict['tech_poc'] = self._scalar_or_entity_id(tech_poc)
        if steward:
            update_dict['steward'] = self._scalar_or_entity_id(steward)
        
        if update_dict:
            resp = self.client.update_field(
                data_store_id=self.data_store_id,
                data_schema_id=self.data_schema_id,
                field_id=self.id,
                field_updates=update_dict
            )
            self._update_self(resp.get('data_field'))
        return self._obj
    

    def get_field_values(self, refresh: bool = False) -> List:
        """Retrieves all field values for this field from Tree Schema
        """
        if refresh or not self._field_values_retrieved:
            if refresh:
                self._reset_field_values()
            field_value_results = self.client.get_all_values_for_field(
                data_store_id=self.data_store_id,
                data_schema_id=self.data_schema_id,
                field_id=self.id
            )
            self._field_values_retrieved = True
            for val in field_value_results:
                found_val = FieldValue(
                    val, 
                    data_store_id=self.data_store_id,
                    data_schema_id=self.data_schema_id,
                    field_id=self.id
                )
                self._add_field_value(found_val)

        return self.field_values

    def field_value(
        self, 
        field_value_inputs: [int, Dict],
        refresh: bool = False,
        raise_if_not_exist: bool = False
    ) -> FieldValue:
        """Creates or retrieves a field value object, Inputs can be an integer 
        (for the field ID), a string (for the sample value) or a dictionary 
        of values used to create the field value

        :param field_inputs: the inputs used to create or retrieve
            the field
        :param refresh: whether or not to force a refresh from the database,
            the default is False
        :param raise_if_not_exist: default is False, if True will raise a 
            `treescheam.exceptions.DataAssetDoesNotExist` exception if the field value 
            does not exists, when False `None` is returned for field values that
            do not exist

        :returns: a `FieldValue` object

        >>> field_value = my_field.field_value(1)
        >>> field_value = my_field.field_value('101')
        >>> field_value = my_field.field_value({
                'field_value': '02', 
                'description': 'a status code value'
            })

        The required fields are managed by the API, all required fields for  
        sample values can be found in BODY of the the API to 
        `Create a Field Value <https://developer.treeschema.com/rest-api/#create-a-field-value>`_
        """
        # Pre-fetch all field values for the field on the first retrival
        self._check_retrieve_field_values(refresh)

        field_value = None
        if (isinstance(field_value_inputs, int) 
            and field_value_inputs in self._field_values_by_id):
            field_value = self._field_values_by_id[field_value_inputs]
        elif (isinstance(field_value_inputs, str) 
            and field_value_inputs.lower() in self._field_values_by_value):
            field_value = self._field_values_by_value[field_value_inputs.lower()]
        elif isinstance(field_value_inputs, dict):
            field_value = FieldValue(
                field_value_inputs, 
                data_store_id=self.data_store_id,
                data_schema_id=self.data_schema_id,
                field_id=self.id
            )
            self._add_field_value(field_value)

        if raise_if_not_exist and not field_value:
            raise DataAssetDoesNotExist(
                    'The field value requested: %s does not exist' % field_value_inputs
                ) 

        return field_value

    def _clean_field_inputs(self, d):
        """Converts native python types to string representations
        for the data format field 
        """
        _type = d.get('type')
        if not ((isinstance(_type, str) and _type in self._eligible_string_types)
            or any([_type == t for t in self._eligible_native_types_map.keys()])):
            msg = (
                """The data type provided is invalid. Must provide a string value
                that is one of %s or provide one of the following native python types: %s
                """ % (
                    self._eligible_string_types, 
                    [x.__name__ for x in list(self._eligible_native_types_map.keys())]
                )
            )
            raise InvalidFieldInputs(msg)

        if _type and not isinstance(_type, str):
            if _type == dict:
                new_type = 'object'
            elif _type == list:
                new_type = 'list'
            else:
                new_type = 'scalar'
            d['type'] = new_type

        # Allow the user to just provide a native python type for the type
        data_type = d.get('data_type')
        if data_type is None and _type in self._eligible_native_types_map.keys():
            data_type = _type

        if data_type and not isinstance(data_type, str):
            # Default to string
            d['data_type'] = self._eligible_native_types_map.get(data_type, 'string')


        # Allow the user to just provide a native python type for the type
        data_format = d.get('data_format')
        if data_format is None and _type in self._eligible_native_types_map.keys():
            data_format = _type

        if (data_format 
            and not isinstance(data_format, str)
            and hasattr(data_format, '__name__')):
            data_format = data_format.__name__
            d['data_format'] = data_format
        elif isinstance(data_format, str):
            d['data_format'] = data_format
        else:
            d['data_format'] = 'unknown'
