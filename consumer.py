from kafka import KafkaConsumer
import os

boot_server = os.environ['boot_server']
consumer = KafkaConsumer('gold', bootstrap_servers=[boot_server])

for x in consumer:
    print(x)

