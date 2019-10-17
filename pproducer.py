#!/usr/bin/env python3

#
# Simple kafka producer
#

import time
import datetime
from confluent_kafka import Producer

# VARs
msg_number = 50000
sleep_time = 0 # throttle time, 0 is none 

# List comprehension
#data_source = [f'msg-{x}' for x in range(0, msg_number)]

# Generator expression
data_source = (f'msg-{x}' for x in range(0, msg_number))

producer_configs = {
   'bootstrap.servers': 'kb1-tds:9092,kb2-tds:9092,kb3-tds:9092',
   'security.protocol': 'sasl_ssl',
   'ssl.endpoint.identification.algorithm': 'none',
   'enable.ssl.certificate.verification': 'true',
   'ssl.ca.location': './ca-cert',
   'sasl.mechanism': 'PLAIN',
   'sasl.username': 'user1',
   'sasl.password': 'passwd1',
   'queue.buffering.max.messages': 1000000,
   'queue.buffering.max.ms': 5000,
}


p = Producer(producer_configs)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))

start_time = time.time()
for data in data_source:
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    p.produce('demo1', data.encode('utf-8'), callback=delivery_report)

    # Throttle
    time.sleep(sleep_time)
# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
stop_time = time.time()

print(f'Sent {msg_number} messages in {stop_time - start_time} seconds')
print(f'Average msg/s: {msg_number / (stop_time - start_time)}')
