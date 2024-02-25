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

# Function to generate a unique identifier
def generate_uuid():
    return uuid.uuid4()

# Setting up a Kafka producer
client = KafkaClient(hosts="localhost:9092")
topic = client.topics['geodata_stream_topic_123']
producer = topic.get_producer()

# Function to construct messages from coordinate data
def construct_messages(coordinates):
    messages = []
    for i in range(len(coordinates)):
        data = {
            'service': 'alpha',
            'key': 'alpha_' + str(generate_uuid()),
            'datetime': str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
            'unit': coordinates[i]['unit'],
            'latitude': coordinates[i]['coordinates'][1],
            'longitude': coordinates[i]['coordinates'][0]
        }
        message = json.dumps(data)
        messages.append(message)
    return messages

# Function to send messages in batches to Kafka and measure ingestion rate
def generate_checkpoint(messages, batch_size):
    batch = []
    m = len(messages)
    start_time = time.time()
    for message in messages:
        batch.append(message)
        if len(batch) >= batch_size:
            for msg in batch:
                producer.produce(msg.encode('ascii'))
            batch = []
    # Sending remaining messages in the last batch
    for msg in batch:
        producer.produce(msg.encode('ascii'))
    # Logging the data ingestion rate
    print("Data ingestion rate for batch size {} equals {}".format(batch_size, m / (time.time() - start_time)))

# Constructing messages for each route
messages1 = construct_messages(coordinates1)
messages2 = construct_messages(coordinates2)
messages3 = construct_messages(coordinates3)
messages4 = construct_messages(coordinates4)

# Combining messages from different routes
messages = messages1 + messages2 + messages3 + messages4

# Testing different batch sizes to determine the optimal ingestion rate
batch_sizes = [1, 5, 10, 20, 50]
for batch_size in batch_sizes:
    generate_checkpoint(messages, batch_size)
