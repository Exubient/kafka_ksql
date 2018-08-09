from kafka import KafkaProducer
import json
import time
import os

boot_server = os.environ['boot_server']
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers=[boot_server])

for index in range(100000):
    print('{} has been added'.format(index))
    producer.send('gold', key=b'test_key', value={'gold':index})
    time.sleep(3)

