from http import client
import pulsar
import os

broker_host = os.environ.get('PULSAR_URL')
topic = os.environ.get('PULSAR_TOPIC')

client = pulsar.Client(broker_host)

producer = client.create_producer(
    topic, 
    block_if_queue_full=True, 
    batching_enabled=True, 
    batching_max_publish_delay_ms=10
)

def producer_callback(res, msg_id):
    print(f"message published {msg_id}}.")

i = 0
while True:
    producer.send_async(f'Hello-{i}'.encode('utf-8'), producer_callback)
    i+=1