#!/usr/bin/env python3

#
# Simple Kafka consumer
#

import time
from confluent_kafka import Consumer, KafkaError

consumer_configs = {
   'bootstrap.servers': 'kb1-tds:9092,kb2:9092,kb3-tds:9092',
   'security.protocol': 'sasl_ssl',
   'ssl.endpoint.identification.algorithm': 'none',
   'enable.ssl.certificate.verification': 'true',
   'ssl.ca.location': './ca-cert',
   'sasl.mechanism': 'PLAIN',
   'sasl.username': 'user1',
   'sasl.password': 'passwd1',
   'group.id': 'mygroup',
}


c = Consumer(consumer_configs)
c.subscribe(['demo1'])

# Set counters
stime = time.time()
hits = 0
hitsPerSecond = 0

# Loop and read
while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    hits = hits + 1

    # Counter
    if time.time() >=  stime + 1:
        hitsPerSecond = hits / 1
        stime = time.time()
        hits = 0

    print(f"Received message: {msg.value().decode('utf-8'):10} ... hits/s = {hitsPerSecond}", end='\r', flush=True)

c.close()
