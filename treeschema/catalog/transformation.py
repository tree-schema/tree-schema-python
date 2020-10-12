from typing import Any, Dict, List, Tuple

from . import (
    DataField, 
    TransformationLink, 
    TreeSchemaSerializer, 
    TreeSchemaUser
)
from .tags import get_tags_added
from ..exceptions import DataAssetDoesNotExist, InvalidLinksException


class Transformation(TreeSchemaSerializer):
    """An object that represents a single data store."""
    __ID_FIELD_NAME__ = 'transformation_id'
    __NAME_FIELD__ = 'name'
    __FIELDS__ = {
        'created_ts': str,
        'description_markup': str,
        'description_raw': str,
        'name': str,
        'steward': lambda x: TreeSchemaUser(x),
        'tech_poc': lambda x: TreeSchemaUser(x),
        'transformation_id': int,
        'type': str,
        'updated_ts': str
    }

    def __init__(
        self,
        transformation_inputs: [int, Dict],
        *args, 
        **kwargs
    ):
        """Create a data store object with either the ID of a data store
        or the fully defined data store object as a dictionary.

        :param id: the ID for the data store
        :param inputs: a dictionary of inputs that can 
        fully serialize a data store
        """
        self.tags = []
        self._links_by_id = {}
        self._links_retrieved = False
        super(Transformation, self).__init__(transformation_inputs)

    def _get_self_by_id(self):
        raw_resp = self.client.get_transformation_by_id(self.id)
        return raw_resp.get('transformation')

    def _get_self_by_name(self):
        raw_resp = self.client.get_transformation_by_name(self._name)
        return raw_resp.get('transformation')

    def _add_link(self, link: TransformationLink) -> None:
        """Adds a link value to the internal mappings"""
        self._links_by_id[link.id] = link

    def _remove_link(self, link_id: int) -> None:
        """Removes a link from the internal mappings"""
        self._links_by_id.pop(link_id, None)

    def _reset_links(self) -> None:
        """Sets the internal mappings"""
        self._links_by_id = {}
    
    def _check_retrieve_links(self, force_refresh=False):
        if not self._links_retrieved or force_refresh: 
            self.get_links()

    @property
    def links(self):
        """A dictionary of links that have been retrieved from Tree Schema. This
        will not have all of the links for a given transformation until 
        `get_links()` is called to fetch the existing links.
        """
        self._check_retrieve_links()
        return self._links_by_id

    def add_tags(self, tags: List[str]) -> Dict:
        """Adds one or more tags to the transformation

        :param tags: a list of tags, a single tag can also be passed
        :returns: the API response

        >>> my_transformation = ts.transformation('my transformation')
        >>> my_transformation.add_tags(['new_tag', 'a second new tag'])
        >>> my_transformation.add_tags('single tag')
        """
        if not isinstance(tags, list):
            tags = [tags]

        resp = None
        tags_to_add = [t for t in tags if t not in self.tags]
        if len(tags_to_add) > 0:
            tag_res = self.client.add_tag_to_data_store(self.id, tags_to_add)
            added_tags = get_tags_added(tag_res)
            self.tags.extend(added_tags)
            resp = tag_res
        return resp
        
    def _create(self):
        transformation = {}
        if not self._is_validated:
            transformation_raw = self.client.create_transformation(self._raw_inputs)
            transformation = transformation_raw.get('transformation')
            if transformation:
                self._is_validated = True
                self.id = transformation['transformation_id']
        return transformation

    def get_links(self, refresh: bool = False) -> List:
        """Retrieves all transformation links for the transformation. 
        After this is called for the first time the links are cached locally.

        :param refresh: Default False, if True, will force all links 
            to be retrieved from Tree Schema and not the local cache
        :returns: a list of `TransformationLink` objects that belong to 
            this transformation
        """
        if refresh or not self._links_retrieved:
            if refresh:
                self._reset_links()
            link_results = self.client.get_all_transformation_links(self.id)
            self._links_retrieved = True
            for link in link_results:
                found_link = TransformationLink(
                    link, 
                    transformation_id=self.id
                )
                self._add_link(found_link)
            
        return self.links

    def _get_single_link(self, source, target):
        """Creates a single transformation link structure"""
        return {
            'source_field_id': source,
            'target_field_id': target
        }
    
    def _is_transform_link_obj(self, obj):
        if isinstance(obj, dict):
            if ('source_field_id' in obj.keys() 
                and 'target_field_id' in obj.keys()
                and len(obj.keys()) == 2):
                return True
        return False

    def _is_transform_field_obj(self, obj):
        if (isinstance(obj, tuple) or isinstance(obj, list)) and len(obj) == 2:
            if all([type(x) == DataField for x in obj]):
                return True
        return False
        

    def _get_link_structure(self, links):
        """Validates and formats the links from the input"""
        _links = None
        if self._is_transform_link_obj(links):
            _links = [links]
        elif self._is_transform_field_obj(links):
            source = self._scalar_or_entity_id(links[0])
            target = self._scalar_or_entity_id(links[1])
            _links = [self._get_single_link(source, target)]
        elif isinstance(links, list) and len(links) > 0:
            if all([self._is_transform_link_obj(l) for l in links]):
                _links = links
            elif all([self._is_transform_field_obj(l) for l in links]):
                _links = [
                    self._get_single_link(
                        self._scalar_or_entity_id(lnk[0]),
                        self._scalar_or_entity_id(lnk[1])
                    ) for lnk in links
                ]
        return _links

    def _create_or_set_links_state(
        self, 
        links: [
            Dict, 
            List[Dict], 
            Tuple[DataField, DataField],
            List[Tuple[DataField, DataField]]
        ],
        set_state=False
    ) -> List[Dict]:
        """Creates a set set of links or refreshes the state of all links within
        the transformation.
        """
        links = self._get_link_structure(links)
        if links:
            link_data = {'links': links}
            link_results_raw = self.client.create_transformation_links(
                self.id,
                links=link_data,
                set_state=set_state
            )
            self._links_retrieved = True
            link_results = link_results_raw.get('updated_links')
            for link in link_results:
                found_link = TransformationLink(
                    link, 
                    transformation_id=self.id
                )
                self._add_link(found_link)
        else:
            _msg = """To create new links the input must be in one of the following:

            - A single dictionary of source and target field IDs:
                {'source_field_id': 1, 'target_field_id': 2}

            - A list of dictionaries of source and target field IDs:
                [{'source_field_id': 1, 'target_field_id': 2}]
            
            - A single Tuple of Data Fields, position 0 = source & position 1 = target
                (DataField, DataField)

            - A list of Tuples of Data Fields, position 0 = source & position 1 = target
                [(DataField, DataField), (DataField, DataField)]
            """
            raise InvalidLinksException(_msg)
        return self.links

    def create_links(
        self, 
        links: [
            Dict, 
            List[Dict], 
            Tuple[DataField, DataField],
            List[Tuple[DataField, DataField]]
        ]
    ) -> List[Dict]:
        """Creates a `TransformationLink` between two `DataField`s. The 
        `TransformationLink` is the building block for data lineage and 
        describes how data moves from one schema to another.
        
        :param links: a single dictionary containing data to create or 
            retrieve a link or a list of dictionaries

        >>> src_schema = ts.data_store('my 1st data store').schema('my.schema1')
        >>> tgt_schema = ts.data_store('another data store').schema('schema.num2')
        >>>
        >>> t = ts.transformation('my transform')
        >>> transform_links = [
        >>>     (src_schema_1.field('field_1'), tgt_schema_1.field('target_field'))
        >>> ]
        >>> t.create_links(transform_links) 
        """
        return self._create_or_set_links_state(links, False)


    def set_links_state(
        self, 
        links: [
            Dict, 
            List[Dict], 
            Tuple[DataField, DataField],
            List[Tuple[DataField, DataField]]
        ]
    ): 
        """Sets the current state of the traansformation to have exactly 
        the links provided as input. Any exiting links that are not 
        provided in the input but exist within the transformation will be 
        deprecated and any new links provided that do not exist within the 
        transformation will be created.

        :param links: a single dictionary containing data to create or 
            retrieve a link or a list of dictionaries

        >>> src_schema = ts.data_store('my 1st data store').schema('my.schema1')
        >>> tgt_schema = ts.data_store('another data store').schema('schema.num2')
        >>>
        >>> t = ts.transformation('my transform')
        >>> transform_links = [
        >>>     (src_schema_1.field('field_1'), tgt_schema_1.field('target_field'))
        >>> ]
        >>> t.set_links_state(transform_links)
        """
        return self._create_or_set_links_state(links, True)

    def link(
        self, 
        link_inputs: [
            Dict, 
            List[Dict], 
            Tuple[DataField, DataField],
            List[Tuple[DataField, DataField]]
        ],
        refresh: bool = False,
        raise_if_not_exist: bool = False
    ):
        """Creates or retrieves a transformation link object, inputs 
        can be an integer (for the transformation link ID) or a 
        dictionary of values used to create the link

        :param link_inputs: the inputs used to create or retrieve
            the link
        :param refresh: whether or not to force a refresh from the database,
            the default is False
        :param raise_if_not_exist: default is False, if True will raise a 
            `treescheam.exceptions.DataAssetDoesNotExist` exception if the link 
            does not exists, when False `None` is returned for link that
            do not exist
        :returns: a Transformation Link object

        >>> t = ts.transformation('my transform')
        >>> link1 = t.link(1)
        >>> link2 = t.link({'source_field_id': 1, 'target_field_id': 2})
        """
        # Pre-fetch all links for the transformation on the first retrival
        self._check_retrieve_links(refresh)

        link = None
        if (isinstance(link_inputs, int) 
            and link_inputs in self._links_by_id):
            link = self._links_by_id[link_inputs]
        elif isinstance(link_inputs, dict):
            link = TransformationLink(link_inputs, transformation_id=self.id)
            self._add_link(link)

        if raise_if_not_exist and not link:
            raise DataAssetDoesNotExist(
                    'The link requested: %s does not exist' % link_inputs
                ) 
        return link

    def delete_links(self, remove_links: [List[int], int]) -> bool:
        """Deletes (deprecates) a transformation link, or list of links,
        from a transformation.

        :param remove_links: a single link or a list of link (these 
            can be the link ID or a list of `TransformationLink` objects)
        :returns: True if the links are deprecated

        >>> my_transformation = ts.transformation('my transform')
        >>> delete_link1 = my_transformation.link(1)
        >>> delete_link2 = my_transformation.link(2)
        >>> my_transformation.delete_links([delete_link1, delete_link2])
        True
        """
        if not isinstance(remove_links, list):
            remove_links = [remove_links]

        _scalar_links = [self._scalar_or_entity_id(i) for i in remove_links]
        delete_links = {'transform_link_ids': _scalar_links}
        deleted = self.client.delete_links_from_transformation(
            self.id, 
            delete_links=delete_links
        )
        if deleted:
            for tlid in _scalar_links:
                self._remove_link(tlid)
        return deleted

