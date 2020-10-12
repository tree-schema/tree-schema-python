import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'user-output-aggregations.v1', 
    bootstrap_servers=['localhost:9092']
)

for msg in consumer:
    print (json.loads(msg.value))