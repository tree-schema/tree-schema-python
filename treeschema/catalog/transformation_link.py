from typing import Any, Dict, List

from . import TreeSchemaSerializer


class TransformationLink(TreeSchemaSerializer):
    """An object that represents a data movement between 
    two fields in different data schemas.
    """
    __ID_FIELD_NAME__ = 'transformation_link_id'
    __FIELDS__ = {
        'created_ts': str,
        'source_data_store_id': int,
        'source_data_store_name': str,
        'source_schema_id': int,
        'source_schema_name': str,
        'source_field_id': int,
        'source_field_name': str,
        'target_data_store_id': int,
        'target_data_store_name': str,
        'target_schema_id': int,
        'target_schema_name': str,
        'target_field_id': int,
        'target_field_name': str,
        'transformation_link_id': int,
        'updated_ts': str,
    }

    def __init__(
        self,
        transformation_link_inputs: [int, Dict],
        transformation_id,
        *args, 
        **kwargs
    ):
        """Create a transformation link object with either the ID of 
        the transformation link or the fully transformation link 
        object as a dictionary.

        :param transformation_id: the ID for the transformation link
        :param transformation_link_inputs: the ID for the transformation
            link or a dictionary of inputs that can be used to create
            a link
        """
        self.transformation_id = transformation_id
        super(TransformationLink, self).__init__(transformation_link_inputs)
    
    def _get_self_by_id(self):
        raw_resp = self.client.get_transformation_link_by_id(
            transformation_id=self.transformation_id,
            transformation_link_id=self.id
        )
        return raw_resp.get('transformation_link')
    
    def _create(self):
        raise Exception('This should never happen')



