from treeschema import TreeSchema
ts = TreeSchema('your@email.com', 'your_tree_schema_secret_key')   

# Get a data store # note - the data store must be eligible for dbt processing
ds = ts.data_store(1)

##### Process a local file

# Define the manifest file location
f_loc = './manifest.json'

# Send the manifest to Tree Schema to be parsed
ds.dbt.parse_dbt_manifest(f_loc)
ds.dbt.wait_for_parse_complete()
ds.dbt.get_manifest_parse_status()

# Save the results
ds.dbt.save_parse_results(
    add_schemas_fields=True,
    update_descriptions=True,
    update_tags=True,
    add_lineage=True
)

##### Process a manifest file on another server

# Read the file as bytes (reading a local file here but the same applies to S3, etc.)
with open(f_loc, 'rb') as f:
    manifest = f.read()

# Validate that the manifest is JSON
json.loads(manifest)

# Send the manifest to Tree Schema
ds.dbt.parse_dbt_manifest(manifest)
ds.dbt.wait_for_parse_complete()

ds.dbt.save_parse_results(
    add_schemas_fields=True,
    update_descriptions=True,
    update_tags=True,
    add_lineage=True
)

