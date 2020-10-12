import datetime 
import json 
import random 
import time  

from kafka import KafkaProducer 
from kafka.errors import KafkaError 

producer = KafkaProducer(bootstrap_servers=['localhost:9092']) 
TOPIC_NAME = 'user-input-data.v1' 
RAND_USERS = [f'user_{i}' for i in range(20)]


def generate_ranom_msg(): 
    data = { 
        "user_id": random.choice(RAND_USERS), 
        "rand_val": random.random() 
    } 
    return json.dumps(data).encode('utf-8') 


while True: 
    time.sleep(2) 
    msg = generate_ranom_msg()
    print('sending msg: %s @ %s' % (msg, time.time()))
    future = producer.send(TOPIC_NAME, msg) 
