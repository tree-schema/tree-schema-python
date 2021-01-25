import json
import logging
import os
import time

from ..api import APIClient
from ..exceptions import (
    DbtManifestInvalid,
    DbtManifestNotParsed,
    InvalidManifestParseStatus,
    ManifestParseWaitTimeout
)

logger = logging.getLogger(__name__)


class DbtManager(object):

    def __init__(self, data_store_id: int):
        """Create a dbt manager that handles the sending of a manifest
        file to Tree Schema, waits for the parsing to complete,
        caches the result output from the parsing and submits results
        to be saved in Tree Schema

        :param data_store_id: the ID for the data store
        """
        self.client = APIClient()
        self.data_store_id = data_store_id


    def parse_dbt_manifest(self, manifest: [str, bytes]) -> str:
        """Sends a manifest file to Tree Schema to be parsed.

        :param manifest: The dbt manifest file to send to Tree Schema.
            This can be a string which points to a location where the 
            file can be read, or it can be in the form of bytes where the
            user would have already read the object into memory, for example
            if the object resides on an external file store.
        :returns: a string, the dbt_process_id, representing the unique
            parse instance

        >>> my_data_store = ts.data_store('my data store')
        >>> my_data_store.dbt.parse_dbt_manifest('./path/to/target/manifest.json')
        """
        _manifest_content = None
        if isinstance(manifest, str):
            if not os.path.isfile(manifest):
                raise DbtManifestInvalid(
                    'Could not find a the corresponding file for the location: %s' % manifest
                )
            with open(manifest,'rb') as f:
                _manifest_content = f.read()
        elif isinstance(manifest, bytes):
            _manifest_content = manifest

        if _manifest_content is None:
            raise DbtManifestInvalid(
                'The manifest must be a string, pointing to a file location or bytes ' 
                'that represent the content of the file.'
            )
        try:
            json.loads(_manifest_content)
        except json.JSONDecodeError:
            raise DbtManifestInvalid(
                'The manifest must be valid JSON. The content provided could not be converted from JSON.'
            )

        self.dbt_process_id = self.client.parse_dbt_file(
            data_store_id=self.data_store_id,
            manifest_content=_manifest_content
        )
        return self.dbt_process_id

    def get_manifest_parse_status(self) -> str:
        """Retrieves the manifest parse status from Tree Schema as well 
        as the results from the parse, if the parse has been completed.

        :returns: The manifest parse status
        """
        if not hasattr(self, 'dbt_process_id'):
            raise DbtManifestNotParsed(
                'Parse a dbt manifest.json file before calling this function'
            )

        parse_results = self.client.get_dbt_parse_status(
            dbt_process_id=self.dbt_process_id
        )
            
        self.parse_status = parse_results['status']
        self.parse_error = parse_results['error_msg']
        self.parsed_schemas = parse_results['dbt_schemas']
        self.parsed_lineage = parse_results['dbt_lineage']

        return self.parse_status

    def wait_for_parse_complete(self, max_seconds=360) -> str:
        """Parsing takes place asynchronously, this function acts as a waiter 
        to try and retrieve the status of the parse for up to max seconds 
        (default 5 minutes) before timing out.

        :param max_seconds: the maximum number of seconds to wait while 
            waiting for the results
        :returns: the final parse status

        >>> my_data_store = ts.data_store('my data store')
        >>> my_data_store.dbt.parse_dbt_manifest('./path/to/target/manifest.json')
        >>> my_data_store.dbt.wait_for_parse_complete()
        """
        parse_complete = False
        parse_status = None
        now = 0
        start_time = time.time()
        while not parse_complete:
            parse_status = self.get_manifest_parse_status()
            if parse_status != 'waiting':
                parse_complete = True
            else:
                logger.debug('Received parse status: %s' % parse_status)
                now = time.time()
                if now - start_time > max_seconds:
                    ManifestParseWaitTimeout(
                        'The manifest file has not yet completed and it has been '
                        'more than the maximum amount of time: %s seconds.' % max_seconds
                    )
                else:
                    time.sleep(1)
        return parse_status

    def save_parse_results(
        self, 
        add_schemas_fields: bool = False,
        update_descriptions: bool = False,
        update_tags: bool = True,
        add_lineage: bool = True
    ) -> bool:
        """Saves the parsing results into Tree Schema. Allows the user to specify
        the options for saving the results. Users can choose to add the schemas &
        fields, descriptions tags and data lineage indepdently.

        :param add_schemas_fields: whether or not to add the schemas and fields, it
            is suggested to set this to `False` for most use-cases and instead to 
            use the automated crawlers within Tree Schema to extract your schemas and 
            fields first
        :param update_descriptions: whether or not to update descriptions of your data
            assets based off of your dbt manifest file
        :param update_tags: whether or not to update tags of your data
            assets based off of your dbt manifest file
        :param update_tags: whether or not to add lineage to your data
            assets based off of your dbt manifest file
        :returns: boolean, True if the API invocation was successful

        >>> my_data_store = ts.data_store('my data store')
        >>> my_data_store.dbt.parse_dbt_manifest('./path/to/target/manifest.json')
        >>> my_data_store.dbt.wait_for_parse_complete()
        >>> my_data_store.dbt.save_parse_results(update_descriptions=True, add_lineage=True)
        """
        if not hasattr(self, 'parse_status'):
            raise InvalidManifestParseStatus(
                'A manifest file must be parsed before saving. Call parse_dbt_manifest() '
                'to send a manifest file to Tree Schema to be parsed.'
            )
        if self.parse_status == 'waiting':
            raise InvalidManifestParseStatus(
                'The manifest parse status has not been verified to be completed. Call '
                'wait_for_parse_complete() to wait until the parsing is completed. The '
                'parse status must be "parsed" in order to save the results.'
            )
        
        elif self.parse_status == 'error':
            raise InvalidManifestParseStatus(
                'The manifest parse status returned an error state. The following error '
                'was received: %s' % self.parse_error
            )
        
        elif self.parse_status == 'processed':
            raise InvalidManifestParseStatus(
                'This manifest file has already been processed and saved.'
            )
        
        elif self.parse_status == 'parsed':
            save_resp = self.client.save_dbt_results( 
                dbt_process_id=self.dbt_process_id,
                add_schemas_fields=add_schemas_fields,
                update_descriptions=update_descriptions,
                update_tags=update_tags,
                add_lineage=add_lineage
            )
            return 'dbt_process_id' in save_resp
        
        else:
            raise InvalidManifestParseStatus(
                'Unknown parse status found: %s' % self.parse_status
            )
