import pulsar
import os

def my_listener(consumer, msg):
    print(f"Received message: {msg.data().decode('utf-8')}, id:{msg.message_id()}")
    consumer.acknowledge(msg)

broker_host = os.environ.get('PULSAR_URL')
topic = os.environ.get('PULSAR_TOPIC')
subscription = os.environ.get('PULSAR_SUBSCRIPTION')
#consumer_type = os.environ.get('CONSUMER_TYPE')
#consume_position

client = pulsar.Client(broker_host)

consumer = client.subscribe(
    topic,
    subscription,
    consumer_type=pulsar.ConsumerType.Shared,
    initial_position=pulsar.InitialPosition.Earliest,
    message_listener=my_listener,
    negative_ack_redelivery_delay_ms=60000
)

