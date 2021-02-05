
from treeschema import TreeSchema

ts = TreeSchema('your@email.com', 'your_tree_schema_secret_key')   

##################
# User Functions #
##################
# Get all users
ts.get_users()

# Get a user by ID
usr = ts.user(2)

# Get a user by email
usr = ts.user('grant@treeschema.com')


########################
# Data Store Functions #
########################
# Get all data stores
ts.get_data_stores()

# Get a data store by ID
ds = ts.data_store(1)

# Get a data store by Name
ds = ts.data_store('My API Ds #2')

# Get or create a data store, you can pass in a dictionary that 
# contains the minimum fields to create a data store, if a data 
# store with the same name already exists in Tree Schema that 
# data store will be returned, otherwise the data store will be 
# created
ds2_inputs = {
    'name': 'Oracle DB4', 
    'type': 'oracle',
    'description': 'This data store was created with Python',
    'tech_poc': usr # you can pass in the user ID or a TreeSchemaUser object
}
ds2 = ts.data_store(ds2_inputs)

# Get existing tags for data store
ds.tags

# Add tags to a data store
ds.add_tags('single_tag')
ds.add_tags(['new tag', 'data_store_tag'])


#########################
# Data Schema Functions #
#########################

# Get all schemas by data store
# Note - often times a data store may have hundreds or thousands of schemas, 
# in general you should only use get_schemas() for a data store when you truly
# need to pull all of your data stores locally as this could be rate-limited.
# and take a long time to return. Access data schemas directly by getting them 
# by ID or schema name
ds.get_schemas()

# Get a schema from a data store by ID
schema = ds.schema(1)

# Or by the schema name
schema = ds.schema('my_python.schema')
schema = ds.schema(512)

# Get or create a schema, if a schema already exists in the data store
# with the same name it will be returned, otherwise it will be created
new_schema2 = {
    'name': 'my_python_schema4',
    'type': 'table',
    'description': 'My python description',
    'tech_poc': usr,
    'steward': usr
}
schema2 = ds.schema(new_schema2)

# Remove a schema from a data store (this deprecates a schema)
# You can pass a single schema ID, a list of IDs, a single 
# schema object or a list of schema objects
ds.delete_schemas(509)
ds.delete_schemas([513, 510])
ds.delete_schemas(schema2)
ds.delete_schemas([schema2])

# Tag a schema
schema.add_tags('schema tag')
schema.add_tags(['marketing', 'conversion'])

# All of these keyword args are optional, you can pass in 1 or more
schema.update(
    description='This is a new description',
    steward=usr,
    tech_poc=1,
    _type='parquet'
)


########################
# Data Field Functions #
########################

# Get all fields for a schema
schema.get_fields()

# Get a field from a schema by ID
schema.field(3)

# Or get a field by the name
schema.field('my_field_name')

# Get or create a field, if a field already exists in the schema
# with the same name it will be returned, otherwise it will be created
new_field = {
    'name': 'my_new_field4',
    'type': 'scalar',
    'data_type': 'number',
    'data_format': 'bigint',
    'description': 'My python description',
    'tech_poc': usr
}
field = schema.field(new_field)

# Remove a field from a schema (this deprecates the field)
# You can pass a single field ID, a list of IDs, a single 
# field object or a list of field objects
schema.delete_fields(1) 
schema.delete_fields([2, 3]) 
schema.delete_fields(field) 
schema.delete_fields([field]) 

# Fields can be updated, to update a field you must pass in
# keyword arguments
# Note - If changing the type, we use '_type' as the keyword arg
# as to not override the default python type
field.update(description='Newest description', _type='list')

field_update = {
    'data_format': 'YYYY-MM-DD',
    'data_type': 'boolean' 
}
field.update(**field_update)

# Add tags to a field
field.add_tags('field tag')
field.add_tags(['field tag2', 'mktg tag'])

################
# Field Values #
################

# Get all sample values for a field
field.get_field_values()

# Get a field value by ID
field.field_value(16328)

# Or by the value of the field (which will always be a string)
field.field_value('01')


# Get or create a field value, if a field value already exists in the 
# field with the same value it will be returned, otherwise it will be created
new_field_val = {
    'field_value': '101',
    'description': 'status code 101 has some value'
}
field_val = field.field_value(new_field_val)

# Update an existing field value, you must pass in
# keyword arguments, you can change 'field_value', 
# 'description' or both
field_val.update(field_value='202')
field_val.update(description='second desription')
field_val.update(field_value='303', description='third desription')


###################
# Transformations #
###################

# Get all transformations
ts.get_transformations()

# Get a transformation by ID
t = ts.transformation(1)

# Or by the transformation name
t = ts.transformation('My API Transformation!')

# Get or create a transformation, you can pass in a dictionary that 
# contains the minimum fields to create a transformation, if a 
# transformation with the same name already exists in Tree Schema that 
# transformation will be returned, otherwise the transformation will be 
# created
transforma_inputs = {
    'name': 'My API Transformation!', 
    'type': 'pub_sub_event',
    'description': 'This transformation was created with Python',
    'tech_poc': usr # you can pass in the user ID or a TreeSchemaUser object
}
t2 = ts.transformation(transforma_inputs)

# Add tags to a transformation
t.add_tags('transform tag')
t.add_tags(['a list of', 'transform tags'])


#######################
# Transformation Link #
#######################
# Add links to create data lineage

# Get existing links for a transformation
t.get_links()

# Get a single link
link = t.link(244)

# Create new links, this can be done in 1 of 4 ways
#  
#   1. Pass a single source-target mapping that contains 
#      the IDs for the source field and the target field
#      {"source_field_id": 73, "target_field_id": 2792}
#
#   2. A list of source to target mappings:
#      [
#        {"source_field_id": 73, "target_field_id": 2792},
#        {"source_field_id": 73, "target_field_id": 9999}
#      ]
#
#   3. A tuple of Data Field objects, where the DataField 
#      object at index 0 represents the source and index 1
#      represents the target:
#      (DataField, DataField)
#
#   4. A list of tuples that contain Data Field objects:
#      [
#       (DataField, DataField),
#       (DataField, DataField)
#      ]

# Option 1
new_links1 = {
    "source_field_id": 73,
    "target_field_id": 2790,
}
t.create_links(new_links1)

# Option 2
new_links2 = [
    {
        "source_field_id": 73,
        "target_field_id": 2790,
    },
    {
        "source_field_id": 75,
        "target_field_id": 2792
    }
]
t.create_links(new_links2)

# Option 3
DataField1 = ts.data_store(1).schema(2).field(11)
DataField2 = ts.data_store(3).schema(235).field(2746)
new_links3 = (DataField1, DataField2)

t.create_links(new_links3)

# Option 4
DataField3 = ts.data_store(1).schema(2).field(10)
DataField4 = ts.data_store('Kafka').schema('dvc.info.raw:v1').field('user_id')

new_links4 = [
    (DataField1, DataField2),
    (DataField3, DataField4)
]

t.create_links(new_links4)

# Delete links - a single or list of Link objects or link IDs 
t.delete_links(1)
t.delete_links([3, 7])

all_existing_links = list(t.links.values())
t.delete_links(all_existing_links)

