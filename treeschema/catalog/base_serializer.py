from typing import Any, Dict, List

from ..api import APIClient
from ..exceptions import InvalidInputs


class TreeSchemaSerializer(object):
    """Base class for serializing objects from the 
    Tree Schema API.
    """
    def __init__(self, inputs):
        self._validate_input(inputs)
        this_id, this_name, raw_inputs = None, None, None
        if isinstance(inputs, int):
            this_id = inputs
        elif isinstance(inputs, str):
            this_name = inputs
        else:
            raw_inputs = inputs
            if isinstance(raw_inputs, dict):
                if 'steward' in raw_inputs:
                    raw_inputs['steward'] = (
                        self._scalar_or_obj(raw_inputs['steward'])
                    )
                if 'tech_poc' in raw_inputs:
                    raw_inputs['tech_poc'] = (
                        self._scalar_or_obj(raw_inputs['tech_poc'])
                    )

        self.client = APIClient()
        self.id = this_id
        self._name = this_name
        self._raw_inputs = raw_inputs.copy() if isinstance(raw_inputs, dict) else None
        self._is_validated = False
        self.obj

    def __repr__(self):
        if hasattr(self, '_obj') and isinstance(self._obj, dict):
            repr_items = []
            for k,v in self._obj.items():
                repr_items.append('%s: %s' % (k, v))
            res_str = ''
            if len(repr_items) > 0:
                res_str += '\n    '
            res_str += ",\n    ".join(repr_items)
            if len(repr_items) > 0:
                res_str += '\n'
            _res = f'{self.__class__.__name__}({res_str})' 
        else:
            _res = ''
        return _res
    
    def _validate_input(self, _input):
        if all([not isinstance(_input, func) for func in (int, str, dict)]):
            _msg = 'Must provide an integer, string or dictionary to create object'
            raise InvalidInputs(_msg)
        
    def _serialize_obj(self, resp_obj: Dict) -> None:
        """Serializes the response to a set of attributes for 
        the given entity.

        :param resp_obj: A dictionary of values that corresponds 
        to results for the given entity.
        """
        for f, func in self.__FIELDS__.items():
            curr_val = resp_obj[f]
            if curr_val is not None:
                v = func(resp_obj[f])
                setattr(self, f, v)
                resp_obj[f] = v

    def _all_valid_inputs(self, inputs: Dict) -> bool:
        """Checks to see if all of the inputs provided are validated
        as a "complete" Tree Schema object
        """
        for f in self.__FIELDS__.keys():
            if f not in inputs.keys():
                return False
        return True

    @property
    def obj(self) -> Dict[str, Any]:
        """Checks to see if the object already exists locally
        or if it can be built and validated using each class'
        required fields.

        :returns: an existing object of the specified class if
            it exists, otherwise None
        """
        if hasattr(self, '_obj'):
            this_obj = self._obj
        else:
            if self._raw_inputs:
                # Raw inputs can come from a list data stores
                if self._all_valid_inputs(self._raw_inputs):
                    self.id = self._raw_inputs[self.__ID_FIELD_NAME__]
                    self._is_validated = True
                    self._serialize_obj(self._raw_inputs)
                    this_obj = self._raw_inputs
                # Or the user can manually provide values, in this
                # case we need to create the object
                else:
                    this_obj = self._create()
                    self._serialize_obj(this_obj)
            else:
                if self._name:
                    this_obj = self._get_self_by_name()
                else:
                    this_obj = self._get_self_by_id()
                
                if this_obj:
                    self._is_validated = True
                    self._serialize_obj(this_obj)
            
            if not hasattr(self, self.__ID_FIELD_NAME__) or self.id is None:
                self.id = this_obj[self.__ID_FIELD_NAME__]
            if hasattr(self, '__NAME_FIELD__'):
                if not hasattr(self, self.__NAME_FIELD__):
                    setattr(self, self.__NAME_FIELD__, this_obj[self.__NAME_FIELD__])
                if self._name is None:
                    self._name = this_obj[self.__NAME_FIELD__]

        self._obj = this_obj
        return self._obj

    def _update_self(self, new_obj):
        """Updates the underlying attributes for itself"""
        if self._all_valid_inputs(new_obj):
            del self._obj
            self._raw_inputs = new_obj
        self.obj

    @NotImplementedError
    def _get_self_by_id(self):
        """Retrieves itself from the DB using the ID attribute
        available within itself. 
        """
        pass

    @NotImplementedError
    def _get_self_by_name(self):
        """Retrieves itself from the DB using the name attribute
        available within itself.
        """
        pass

    @NotImplementedError
    def _create(self):
        """Creates a serialized object from a set of inputs
        """
        pass

    def _scalar_or_entity_id(self, item):
        """Allows the user to pass in an instance of the base 
        serializer or an ID that referes to an object in 
        Tree Schema and the serializer will return the correct
        ID. 
        
        For example, when deleting a field from a schema you
        may pass in `1` for the field ID or you may pass in the 
        `DataField` object which refers to the field ID. 
        This allows you to pass in either and the serializer 
        will return the correct response
        """
        _resp = None
        if item:
            if isinstance(item, TreeSchemaSerializer):
                if hasattr(item, 'id'):
                    _resp = item.id
            else:
                _resp = item
        return _resp

    def _scalar_or_obj(self, item):
        """Allows the user to pass in an instance of the base 
        serializer or an ID that referes to an object in 
        Tree Schema and the serializer will return the ID 
        or the corresponding class if an tree schema object
        is passed.
        
        For example, when associating a user to a field you
        may pass in `1` for the user ID or you may pass in the 
        `TreeSchemaUser` object which refers to the user ID. 
        This allows you to pass in either and the serializer 
        will return the correct response
        """
        _resp = None
        if item:
            if isinstance(item, TreeSchemaSerializer):
                if hasattr(item, '_obj'):
                    _resp = item._obj
            else:
                _resp = item
        return _resp