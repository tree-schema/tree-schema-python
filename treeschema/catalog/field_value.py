from typing import Any, Dict, List

from . import TreeSchemaSerializer


class FieldValue(TreeSchemaSerializer):
    """An object that represents a single data store."""
    __ID_FIELD_NAME__ = 'field_value_id'
    __NAME_FIELD__ = 'field_value'
    __FIELDS__ = {
        'created_ts': str,
        'description_markup': str,
        'description_raw': str,
        'field_value': str,
        'field_value_id': int,
        'updated_ts': str
    }

    def __init__(
        self,
        field_value_inputs: [int, Dict],
        data_store_id: int,
        data_schema_id: int,
        field_id: int,
        *args, 
        **kwargs
    ):
        """Create a data field object with either the ID of a data field
        or the fully defined data field object as a dictionary.

        :param field_value_inputs: the inputs to create or retrieve 
            the data field value
        :param data_store_id: The ID of the data store that this field
            value belongs to
        :param field_id: The ID of the data schema that this field
            value belongs to
        :param field_id: The ID of the data field that this field
            value belongs to
        """
        self.data_store_id = data_store_id
        self.data_schema_id = data_schema_id
        self.field_id = field_id
        super(FieldValue, self).__init__(field_value_inputs)

    def _get_self_by_id(self):
        raw_resp = self.client.get_field_value_by_id(
            data_store_id=self.data_store_id,
            data_schema_id=self.data_schema_id,
            field_id=self.field_id,
            field_value_id=self.id
        )
        return raw_resp.get('field_value')
    
    def _get_self_by_name(self):
        raw_resp = self.client.get_field_value_by_name(
            data_store_id=self.data_store_id,
            data_schema_id=self.data_schema_id,
            field_id=self.field_id,
            value=self._name
        )
        return raw_resp.get('field_value')

    def _create(self):
        field_value = {}
        if not self._is_validated:
            field_value_resp = self.client.create_field_value(
                data_store_id=self.data_store_id, 
                data_schema_id=self.data_schema_id,
                field_id=self.field_id,
                field_value_info=self._raw_inputs
            )
            field_value = field_value_resp['field_value']
            if field_value:
                self._is_validated = True
                self.id = field_value['field_value_id']
        return field_value

    def update(self, 
        *, 
        field_value: str = None, 
        description: str = None
    ):
        """Update an existing field value. Only keyword arguments can be provided, positional 
        arguments are not allowed.

        :param field_value: sample value, must be a Tree Schema field type
        :param description: The description for the field value
        :returns: a `FieldValue`, an updated version of itself
        """

        update_dict = {}
        if field_value:
            update_dict['field_value'] = field_value
        if description:
            update_dict['description'] = description
        
        if update_dict:
            resp = self.client.update_field_value(
                data_store_id=self.data_store_id,
                data_schema_id=self.data_schema_id,
                field_id=self.field_id,
                field_value_id=self.id,
                field_value_updates=update_dict
            )
            self._update_self(resp.get('field_value'))
        return self._obj
