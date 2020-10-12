
import faust
from kafka import KafkaProducer


# Faust Models
class User(faust.Record, serializer='json'):
    user_id: str
    rand_val: float

class UserOutput(faust.Record, serializer='json'):
    user_id: str
    user_cnt: int
    user_max: int
    user_min: int


# Kafka configs
INPUT_TOPIC_NAME = 'user-input-data.v1'
OUTPUT_TOPIC_NAME = 'user-output-aggregations.v1'
BROKER = 'localhost:9092'

# Faust app
app = faust.App(
    'UserProcess',
    broker=f'kafka://{BROKER}', 
    version=1,
    consumer_auto_offset_reset='latest'
)


# Output configs
KAFKA_PRODUCER = KafkaProducer(bootstrap_servers=[BROKER])


# Send messages back to another kafka topic
def publish_message(user_id, user_cnt, user_max, user_min):
    user_output = UserOutput(
        user_id=user_id, 
        user_cnt=user_cnt,
        user_max=user_max,
        user_min=user_min
    )
    key = bytes(user_id, encoding='utf-8')
    KAFKA_PRODUCER.send(
        OUTPUT_TOPIC_NAME, 
        key=key, 
        value=user_output.dumps()
    )
    KAFKA_PRODUCER.flush()


# Incoming topic definition
INPUT_TOPIC = app.topic(INPUT_TOPIC_NAME, value_type=User)

# Table to keep track of per-user events to calculate metrics
TABLE = app.Table(
    'UserAggregationTable', 
    default=list,
    partitions=1
)

@app.agent(INPUT_TOPIC)
async def print_totals(stream):
    """Creates user offers in the database
    """
    async for user in stream:
        user_list = TABLE[user.user_id]
        user_list.append(user.rand_val)
        TABLE[user.user_id] = user_list
        publish_message(
            user_id=user.user_id, 
            user_cnt=len(TABLE[user.user_id]),
            user_max=max(TABLE[user.user_id]),
            user_min=min(TABLE[user.user_id])
        )


# # Tree Schema Configs
# from treeschema import TreeSchema
# ts = TreeSchema('YOUR_EMAIL', 'YOUR_SECRET_KEY')

# # In this example, get or create the schemas. With this function
# # if they do not exist in Tree Schema then they will be created,
# # otherwise the existing schema will be returned
# src_schema_inputs = {'name': INPUT_TOPIC_NAME, 'type': 'json'}
# tgt_schema_inputs = {'name': OUTPUT_TOPIC_NAME, 'type': 'json'}
# src_schema = ts.data_store('Kafka').schema(src_schema_inputs)
# tgt_schema = ts.data_store('Kafka').schema(tgt_schema_inputs)

# # Get or create the fields for each schema
# # You can pass in the native Python data type and the 
# # Tree Schema library will infer the correct values which works
# # well since Faust gives us the field names and data types!
# def get_create_fields(schema, faust_model):
#     for field, dtype in faust_model._options.fields.items():
#         field_obj = {
#             'name': field,
#             'type': dtype,
#             'data_type': dtype,
#             'data_format': dtype.__name__
#         }
#         schema.field(field_obj)

# # Make sure the fields are created for each schema.
# # This will only create the fields the first time it is executed
# get_create_fields(src_schema, User)
# get_create_fields(tgt_schema, UserOutput)

# # Create the data lineage
# transform_links = [
#     (src_schema.field(User.user_id.field), tgt_schema.field(UserOutput.user_id.field)),
    
#     (src_schema.field(User.user_id.field), tgt_schema.field(UserOutput.user_cnt.field)),
    
#     (src_schema.field(User.user_id.field), tgt_schema.field(UserOutput.user_max.field)),
#     (src_schema.field(User.rand_val.field), tgt_schema.field(UserOutput.user_max.field)),
    
#     (src_schema.field(User.user_id.field), tgt_schema.field(UserOutput.user_min.field)),
#     (src_schema.field(User.rand_val.field), tgt_schema.field(UserOutput.user_min.field))
# ]

# # Again, this will create or retrieve the transformation transformation
# transform_inputs = {'name': 'Faust Transform', 'type': 'pub_sub_event'}
# t = ts.transformation(transform_inputs)
# t.set_links_state(transform_links)

