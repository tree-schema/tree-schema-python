"""
This file gives examples for how to wire Tree Schema into your pipeline,
whether that is during your CICD process or within you actual code.
"""

##############################################################
# Example #1: This example shows how to use the Tree Schema  #
#   API in order to create data lineage by explicitly        #
#   defining the source and target fields, this is best      #
#   suited for when your fields are renamed or go through a  #
#   transformation process to create new fields as your data #
#   moves between schemas                                    #
##############################################################

from treeschema import TreeSchema

ts = TreeSchema('your@email.com', 'your_tree_schema_secret_key')

# First, get all users so that they can be assigned as points of contact
# Note - if you have more than 50 users in your org it is more efficient 
#   to just get each user individually in order to prevent pagination in
#   the API, e.g:
#   >>> usr = ts.user('grant@treeschema.com')
# Otherwise, fetching all users first then accessing the ones you want
# is more efficient
ts.get_users()  
usr1 = ts.user('grant@treeschema.com')
usr2 = ts.user('another@treeschema.com')


# Second, generate an object that can be used to create or update
# a transformation object. Remember, the transformation by itself
# is just a shell that is used to hold the transformation links
# and the transformation links are your data lineage. Tree Schema
# will only create the transformation if the transformation name 
# does not already exist, if a transformation does already exist
# it will return the existing transformation. Therefore you can
# run this same function with the same inputs over and over only 
# the first execution will create the transformation.
transforma_inputs = {
    'name': 'My CICD Transformation!', 
    'type': 'pub_sub_event',
    'description': 'This transformation was created with Python',
    'tech_poc': usr1,
    'steward': usr2 
}
t = ts.transformation(transforma_inputs)

# Next, define all of the schemas that will have at least one
# field included in the transformation, for each of these schemas
# we will pre-fetch all of the fields to make accessing 
# subsequent fields more efficient
src_schema_1 = ts.data_store('spoc').schema('spoc.accounts')
src_schema_2 = ts.data_store('spoc').schema('spoc.accounts_audit')
src_schema_3 = ts.data_store('spoc').schema('spoc.accounts_cases')

tgt_schema_1 = ts.data_store('Kafka Dev').schema('blogpost')
tgt_schema_2 = ts.data_store('kafka dev').schema('stori.card.device-session-data.create.raw.v0')
# Note - a single schema can be a source and a target
tgt_schema_3 = src_schema_3

all_schemas = [
    src_schema_1,
    src_schema_2,
    src_schema_3,
    tgt_schema_1,
    tgt_schema_2
]

# Pre-fetch all fields
_ = [schema.get_fields() for schema in all_schemas]

# Finally, define your source and target fields, transformation
# links can be created by passing in a list of tuples where each
# contains two Data Field objects, the Data Field at index 0 is
# the source field and the Data Field at index 1 is the target 
# field

transform_links = [
    (src_schema_1.field('account_type'), tgt_schema_1.field('cast')),
    (src_schema_1.field('annual_revenue'), tgt_schema_1.field('genres')),
    (src_schema_1.field('assigned_user_id'), tgt_schema_1.field('title')),
    (src_schema_1.field('billing_address_city'), tgt_schema_1.field('title')),
    (src_schema_2.field('created_by'), tgt_schema_1.field('genres')),
    (src_schema_2.field('date_created'), tgt_schema_2.field('country_code')),
    (src_schema_2.field('date_created'), tgt_schema_3.field('account_id')),
    (src_schema_3.field('account_id'), tgt_schema_1.field('cast'))
]

# Create the transformation links
# You can call this same function again and again with the same inputs, 
# if a given link already exists within a transformation Tree Schema will
# not create it again, making this ideal to execute in CICD pipelines
# Note - to create links use the create_links function for a transformation:
#   >> t.creat_links(links)
# But to set a transformation to a specific state call the 
# set_links_state function:
#   >> t.set_links_state(links)
# By doing this you will set the transformation to the exact state that you
# provided and you will not need to manage the deletion of links
# TODO: Force to this state
t.create_links(transform_links) 



##############################################################
# Example #2: This example shows how to use the Tree Schema  #
#   API in order to create data lineage for the ETL          #
#   processes where the source and target fields share the   #
#   same name, this is common in scenarios where data is     #
#   extracted to a data lake or an analytical data warehouse.#
#   In these scenarios the data store (or database) and the  #
#   schema name may change but the names of the fields       #
#   remain constant                                          #
##############################################################

# Note - see example 1 above for details one the first few steps
from treeschema import TreeSchema

ts = TreeSchema('grant@treeschema.com', 'password')   

usr1 = ts.user('grant@treeschema.com')
usr2 = ts.user('another@treeschema.com')

transforma_inputs = {
    'name': 'My CICD Transformation!', 
    'type': 'pub_sub_event'
}
t = ts.transformation(transforma_inputs)

# Define the source and target schemas, pre-fetch all of the fields
src_schema = ts.data_store('my data store').schema('usr.accounts')
tgt_schema = ts.data_store('my data store').schema('usr.accounts')

src_schema.get_fields()
tgt_schema.get_fields()

transform_links = []
for src_field in src_schema.fields.values():
    tgt_field = tgt_schema._fields_by_name.get(src_field._name)
    if tgt_field:
        link_tuple = (src_field, tgt_field)
        transform_links.append(src_field, tgt_field)

t.set_links_state(transform_links)
