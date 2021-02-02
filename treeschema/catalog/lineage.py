from typing import Any, Dict, List

from ..api import APIClient
from ..exceptions import InvalidInputs
from .. import treeschema



class LineageImpactSummary(object):
    """Holds the summary for the data assets impacted
    """
    _SUMMARY_FIELDS = ['data_stores', 'schemas', 'fields']

    def __init__(self, impact_summary: Dict[Any, Any]):
        """A summary of the total number of impacted assets

        :param impact_summary: A dictionary containing the summary
            output from the data lineage impact. This should have three
            keys: `data_stores`, `schemas`, and `fields`. The values 
            for each will be integers representing the total number 
            of unique assets of the given type that are impacted.
        
        >>> impacts = {'data_stores': 1, 'schemas': 2, 'fields': 5}
        >>> LineageImpactSummary(impacts)
            LineageImpactSummary(Data Stores: 1, Schemas: 2, Fields: 5)
        """
        self._validate_inputs(impact_summary)
        self._impact_summary_raw = impact_summary
        self.data_stores = impact_summary['data_stores']
        self.schemas = impact_summary['schemas']
        self.fields = impact_summary['fields']

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}('
            f'Data Stores: {self.data_stores}, '
            f'Schemas: {self.schemas}, '
            f'Fields: {self.fields})' 
        )

    def __dict__(self) -> Dict:
        return self._impact_summary_raw

    def _validate_inputs(self, impact_summary) -> None:
        """Validates the inputs contain the correct fields"""
        if (not isinstance(impact_summary, dict)
            or not all ([f in impact_summary for f in self._SUMMARY_FIELDS])
            or not all ([isinstance(k, str) for k in impact_summary.keys()])
            or not all ([isinstance(v, int) for v in impact_summary.values()])
        ):
            raise InvalidInputs(
                'Impact summary must be a dictionary with "data_stores", "schemas" and "fields".'
            )


class ImpactedAsset(object):
    _ASSET_FIELDS = ['data_store_id', 'schema_id', 'field_id']

    def __init__(self, raw_asset: Dict[Any, Any]):
        """A single impacted object. Generated from a raw asset,
        which is a dictionary that contains the IDs required to 
        build a data asset. These data assets will be serialized 
        once the `LineageImpact` has batch retrieved all of the assets.

        Each raw asset may have an impact chain that depicts the broken 
        data lineage that leads to the given asset. For example, if this
        ImpactedAsset is for field `E` and the impact chain is 
        `[B, C, D]` then the full broken lineage will be 
        `B -> C -> D -> E`.

        Note - in order to create an impacted asset you must first 
        instantiate a `TreeSchema()` object

        :param raw_asset: A dictionary of key/value pairs that points
            to a unique data asset within Tree Schema. An impact chain
            may be provided that depicts the broken lineage for this 
            asset

        >>> from treeschema import TreeSchema
        >>> from treeschema.catalog.lineage import ImpactedAsset
        >>> ts = TreeSchema('<your_email>', '<your_secret_key>')
        >>> asset = {
                'data_store_id': 1,
                'schema_id': 1,
                'field_id': 1,
                'impact_chain': [
                    {
                        'data_store_id': 1,
                        'schema_id': 1,
                        'field_id': 1,
                    }
                ]
            }
        >>> ia = ImpactedAsset(asset)
        >>> # After created, Impacted Assets should be serialized
        >>> ia.try_serialize_self()
        >>> ia
            ImpactedAsset(Data Store: Ds1 (1), Schema: DS1 (1), , Field: field_1a (1))
        """
        self.ts = treeschema.TreeSchema()
        self._validate_raw_asset(raw_asset)
        self._raw_asset = raw_asset
        self.processed = False
        self.data_store = None
        self.schema = None
        self.field = None
        self.impact_chain = []

        # Keep track of all raw assets, there may be 
        # more than one asset if there is an impact chain
        # of upstream data assets that leads to this one
        self._all_raw_assets = []
        self._build_full_asset_list()

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}({self.pretty_print_string()})' 
        )

    def __dict__(self) -> Dict:
        return self._raw_asset

    def pretty_print_impact(self, show_by='field') -> str:
        """Generates a list of pretty print strings that can be used to
        show a visual of the impact chain that led to the broken asset.

        :param show_by: Allowed values are: `field` or `schema`. If `field` is provided then
            the impact will be shown at the field level. If `schema` is provided then
            the impact will be shown at the schema level. This may be beneficial to see
            a higher level view in the event that the number of breaking changes for the 
            fields is high and it creates clutter.
        :returns: a string of the pretty printed impact

        >>> print(impacted_asset.pretty_print_impact(show_by='schema'))
            Data Store: Ds1 (1), Schema: ds2 (2), 
                └-->Data Store: Ds1 (1), Schema: ds3 (3), 
                    └-->Data Store: Ds1 (1), Schema: ds4 (4) 
        """
        pretty_print_list = []
        for impacted_asset in self.impact_chain:
            pretty_print_list.append(impacted_asset.pretty_print_string(show_by))
        
        # Add this asset at the end
        pretty_print_list.append(self.pretty_print_string(show_by))
        final_pp_string = ''
        for i, pp_string in enumerate(pretty_print_list):
            if i > 0:
                final_pp_string += '\n'
            line = (' ' * i * 4) + ('└-->' if i > 0 else '') + pp_string
            final_pp_string += line
        return final_pp_string

    def _data_asset_attr(self, asset: Any, attr: str = 'name'):
        """Retrieves a data asset attribute if it exists.

        :param asset: A Tree Schema base serialized object
        :param attr: The name of the attribute
        """
        if hasattr(asset, attr):
            val = getattr(asset, attr)
        else:
            val = None
        return val

    def pretty_print_string(self, show_by: str = 'field') -> str:
        """Creates a pretty printed string for this impacted asset.

        :param show_by: `field`, `schema`. If `field` is provided then
            the impact will be shown at the field level. If `schema` is provided then
            the impact will be shown at the schema level. This may be beneficial to see
            a higher level view in the event that the number of breaking changes for the 
            fields is high and it creates clutter.

        >>> print(impacted_asset.pretty_print_string(show_by='schema'))
            Data Store: Ds1 (1), Schema: ds4 (4)
        """
        pp_string = (
            f'Data Store: {self._data_asset_attr(self.data_store, "name")} '
            f'({self._data_asset_attr(self.data_store, "id")}), '
            f'Schema: {self._data_asset_attr(self.schema, "name")} '
            f'({self._data_asset_attr(self.schema, "id")}) '
        )
        if show_by == 'field':
            pp_string += (
                f', Field: {self._data_asset_attr(self.field, "name")} '
                f'({self._data_asset_attr(self.field, "id")})'
            )
        
        return pp_string

    def try_serialize_self(self):
        """Serializes the response into Tree Schema objects."""
        if not self.processed:
            self.data_store = self.ts.data_store(self._raw_asset['data_store_id'])
            self.schema = self.data_store.schema(self._raw_asset['schema_id'], pre_fetch=False)
            self.field = self.schema.field(self._raw_asset['field_id'], pre_fetch=False)
            self.processd = True
            if 'impact_chain' in self._raw_asset:
                for asset in self._raw_asset['impact_chain']:
                    ia = ImpactedAsset(asset)
                    ia.try_serialize_self()
                    self.impact_chain.append(ia)

    def _validate_raw_asset(self, raw_asset: Dict[str, int]) -> None:
        """Validates the input of the raw asset. This must have 
        a data_store_id, schema_id and field_id and may optionally 
        contain a list of field_ids

        :param raw_asset: The raw asset provided by the API. It 
            must contain the keys 'data_store_id', 'schema_id', 'field_id'
        """
        if not all([f in raw_asset for f in self._ASSET_FIELDS]):
            raise InvalidInputs(
                "Impacted asset must contain the keys 'data_store_id', 'schema_id', 'field_id'" 
            )
        if 'impact_chain' in raw_asset:
            for asset in raw_asset['impact_chain']:
                self._validate_raw_asset(asset)

    def _build_full_asset_list(self) -> None:
        """Builds a complete list of all assets and sub assets that 
        exist within the impact chain for this particular impacted 
        asset. All assets are added to the `self._all_raw_assets` list.
        """
        raw_asset_copy = self._raw_asset.copy()
        impact_chain = raw_asset_copy.pop('impact_chain', [])
        for chain_asset in impact_chain:
            self._all_raw_assets.append(chain_asset)

        self._all_raw_assets.append(raw_asset_copy)


class LineageImpact(object):

    def __init__(self, lineage_impact: Dict[Any, Any]):
        """Represents the impact to data lineage that may 
        occur from a breaking change.

        Converts the response from the `check-breaking-changes`
        endpoint into TreeSchema serialized objects. Lineage 
        impacts allow you to see if there is an expected breaking
        change as well as the full lineage from the original 
        broken chain through each impacted asset.

        >>> from treeschema import TreeSchema
        >>> from treeschema.catalog.lineage import LineageImpact
        >>> ts = TreeSchema('<email>', '<ts_secret_key>')   
        >>> impact = {
                "breaking": True,
                "impact_summary": {
                    "fields": 1,
                    "schemas": 1,
                    "data_stores": 1
                },
                "impacted_assets": [
                    {
                        "field_id": 4,
                        "schema_id": 4,
                        "data_store_id": 1,
                        "impact_chain": [
                            {
                                "field_id": 2,
                                "schema_id": 2,
                                "data_store_id": 1
                            },
                        ]
                    }
                ]
            }
        >>> li = LineageImpact(impact)
        >>> li.breaking
            True

        :param lineage_impact: a dictionary of values
        """
        # Prevent circular import to access TreeSchema singleton
        self.ts = treeschema.TreeSchema()
        self.client = APIClient()
        
        self._lineage_impact_raw = lineage_impact
        self.breaking = None
        self.impact_summary = None
        self.impacted_assets = None
        self._validate_inputs()

    def _validate_inputs(self):
        """Verifies the inputs and converts the assets into 
        Tree Schema serialized objects. Inputs received should
        be in the following format.
        """
        if not isinstance(self._lineage_impact_raw['breaking'], bool):
            raise InvalidInputs('The field "breaking" must be a boolean')
        else:
            self.breaking = self._lineage_impact_raw['breaking']

        self.impact_summary = LineageImpactSummary(
            self._lineage_impact_raw['impact_summary']
        )

        self.impacted_assets = (
            [ImpactedAsset(ia) for ia in self._lineage_impact_raw['impacted_assets']]
        )

        self._serialize_impacted_assets()

    def __repr__(self):
        return (
            f'{self.__class__.__name__}(Breaking: {self.breaking}, {self.impact_summary})'
        )

    def __dict__(self):
        return self._lineage_impact_raw

    def _serialize_impacted_assets(self) -> None:
        """Converts the list of impacted assets into Tree Schema 
        serialized objects, batching requests to Tree Schema to 
        reduce API overhead. 
        """
        if len(self.impacted_assets) > 0:
            # Fields are the most unique attribute within this request,
            # therefore only check to make sure the field is not alredy
            # in the request
            assets_to_serialize = []
            unique_fields = []
            for asset in self.impacted_assets:
                if asset.processed is False:
                    # There may be sub-items if the asset has an impact chain
                    # This ensures all unique assets are retrieved
                    for item in asset._all_raw_assets:
                        item_field_id = item['field_id']
                        if item_field_id not in unique_fields:
                            unique_fields.append(item_field_id)
                            assets_to_serialize.append(item)

            self._serialize_assets(assets_to_serialize)
        
        for ia in self.impacted_assets:
            ia.try_serialize_self()

    def _serialize_assets(self, assets_to_serialize: List[Dict[str, int]]) -> None:
        fields = list(set([f['field_id'] for f in assets_to_serialize]))
        self.ts.batch_load_by_id(field_ids=fields)

    def all_impact_strings(self, show_by: str ='field', show: int = 25) -> str:
        """Creates a string that includes all of the full lineage impact for each impacted 
        asset. For example:

        >>> Data Store: Ds1 (1), Schema: ds2 (2), Field: field_1b (2)
            └-->Data Store: Ds1 (1), Schema: ds3 (3), Field: field_1c (3)
                └-->Data Store: Ds1 (1), Schema: ds4 (4), Field: field_1d (4)
        
        :param show_by: possible values: `field`, `schema`. If `field` is provided then
            the impact will be shown at the field level. If `schema` is provided then
            the impact will be shown at the schema level. This may be beneficial to see
            a higher level view in the event that the number of breaking changes for the 
            fields is high and it creates clutter.
        :param show: The number of assets to show at one time. Default is 25.

        >>> ts = TreeSchema('<email>', '<ts_secret_key>')   
        >>> t = ts.transformation(1)
        >>> links = [ 
                ({'source_field_id': 2, 'target_field_id': 3}),
                ({'source_field_id': 3, 'target_field_id': 4}),
                ({'source_field_id': 4, 'target_field_id': 5})
            ]
        >>> li = t.check_breaking_change(link_state=links)
        >>> printed_impacts = li.all_impact_strings(show=50)
        >>> print(printed_impacts)
        >>> # Lineage for Each Breaking Change
            # --------------------------------
            #
            # Data Store: Ds1 (1), Schema: ds2 (2), Field: field_1b (2)
            #     └-->Data Store: Ds1 (1), Schema: ds3 (3), Field: field_1c (3)
            #
            # -----
            # 
            # Data Store: Ds1 (1), Schema: ds2 (2), Field: field_1b (2)
            #     └-->Data Store: Ds1 (1), Schema: ds3 (3), Field: field_1c (3)
            #         └-->Data Store: Ds1 (1), Schema: ds4 (4), Field: field_1d (4)
        """
        if show_by not in ['field', 'schema']:
            raise InvalidInputs(
                'The argument "show_by" must be one of: "field" or "schema", value "%s" provided'
                % show_by
            )

        filtered_assets = self._get_filtered_impacts(show_by)
        all_impacts_strs = [
            impacted.pretty_print_impact(show_by) for impacted in filtered_assets[:show]
        ]
        header_string = 'Lineage for Each Breaking Change\n--------------------------------\n\n'
                        
        return header_string + '\n\n-----\n\n'.join(all_impacts_strs)

    def _get_filtered_impacts(self, show_by: str = 'field') -> List[ImpactedAsset]:
        """Returns a list of unique impacted assets for the given `show_by` value. 
        If `show_by` is `schemas` then each schema will only show up one time, even 
        if there are multiple impacted fields for that schema.
        
        :param show_by: possible values: `field`, `schema`. If `field` is provided then
            the full list of impacted assets will be returned. If `schema` is provided then 
            one impacted asset for each schema will be returned.
        :returns: A list of impacted assets
        """
        if show_by == 'field':
            filtered_impactes = {impacted.field.id: impacted for impacted in self.impacted_assets}
        elif show_by == 'schema':
            filtered_impactes = {impacted.schema.id: impacted for impacted in self.impacted_assets}
        return list(filtered_impactes.values())
        


