from typing import Any, Dict, List

from . import TreeSchemaSerializer


class TreeSchemaUser(TreeSchemaSerializer):
    """An object that represents a user in your Tree Schema organization"""
    __ID_FIELD_NAME__ = 'user_id'
    __NAME_FIELD__ = 'email'
    __FIELDS__ = {
        'user_id': int,
        'name': str,
        'email': str
    }

    def __init__(
        self,
        user_inputs: [int, Dict],
        *args, 
        **kwargs
    ):
        """Create a data store object with either the ID of a data store
        or the fully defined data store object as a dictionary.

        :param id: the ID for user
        :param inputs: a dictionary of inputs that can 
        fully serialize a data store
        """
        super(TreeSchemaUser, self).__init__(inputs=user_inputs)
        self.obj

    def __str__(self):
        name = self._obj.get('name')
        if name:
            _str = f'{self.__class__.__name__}({name})'
        else:
            _str = ''
        return _str

    def _get_self_by_id(self):
        raw_resp = self.client.get_user(user_id=self.id)
        return raw_resp.get('user')
    
    def _get_self_by_name(self):
        raw_resp = self.client.get_user_by_email(email=self._name)
        return raw_resp.get('user')

    def _create(self):
        user = {}
        if not self._is_validated:
            user = self._get_self_by_id()
            if user:
                self._is_validated = True
                self.id = user['user_id']
        return user
