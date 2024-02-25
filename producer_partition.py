from pykafka import KafkaClient
import json
from datetime import datetime
import uuid
import time

# Reading coordinates from JSON files for different routes
input_file = open('./data/amherst/Route1_30.json')
json_array = json.load(input_file)
coordinates1 = json_array['data']

input_file = open('./data/amherst/Route2_31.json')
json_array = json.load(input_file)
coordinates2 = json_array['data']

input_file = open('./data/amherst/Route3_38.json')
json_array = json.load(input_file)
coordinates3 = json_array['data']

input_file = open('./data/amherst/Route4_33.json')
json_array = json.load(input_file)
coordinates4 = json_array['data']

# Function to generate a unique UUID
def generate_uuid():
    return uuid.uuid4()

# Setting up Kafka producer
client = KafkaClient(hosts="localhost:9092")
topic = client.topics['geodata_stream_topic_123']
producer = topic.get_sync_producer()

# Function to construct messages from coordinate data
def construct_messages(coordinates):
    messages = []
    partition = []
    for coordinate in coordinates:
        data = {
            'service': 'alpha',
            'key': 'alpha_' + str(generate_uuid()),
            'datetime': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            'unit': coordinate['unit'],
            'latitude': coordinate['coordinates'][1],
            'longitude': coordinate['coordinates'][0]
        }
        message = json.dumps(data)
        messages.append(message)
        partition.append(data['unit'])
    return messages, partition

# Function to send messages to Kafka and measure ingestion rate
def generate_checkpoint(messages, partition):
    m = len(messages)
    start_time = time.time()
    for i in range(m):
        producer.produce(messages[i].encode('ascii'), partition_key=str(partition[i]).encode('utf-8'))
    print(m / (time.time() - start_time))

# Constructing and sending messages for each route
messages1, partition1 = construct_messages(coordinates1)
messages2, partition2 = construct_messages(coordinates2)
messages3, partition3 = construct_messages(coordinates3)
messages4, partition4 = construct_messages(coordinates4)

# Combining messages and partitions from different routes
messages = messages1 + messages2 + messages3 + messages4
partition = partition1 + partition2 + partition3 + partition4

# Sending messages to Kafka and printing the ingestion rate
generate_checkpoint(messages, partition)
