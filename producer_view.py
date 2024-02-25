from pykafka import KafkaClient
import json
from datetime import datetime
import uuid
import time

# Load coordinates data from JSON files for different bus routes
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

# Function to generate a unique identifier (UUID)
def generate_uuid():
    return uuid.uuid4()

# Setup a Kafka producer connection
client = KafkaClient(hosts="localhost:9092")
topic = client.topics['geodata_stream_topic_123']
producer = topic.get_sync_producer()

# Data template for the Kafka message
data = {}
data['service'] = 'alpha'

# Function to construct a list of messages for a given set of coordinates
def construct_messages(coordinates):
    messages = []
    for i in range(len(coordinates)):
        data['key'] = data['service'] + '_' + str(generate_uuid())
        data['datetime'] = str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        data['unit'] = coordinates[i]['unit']
        data['latitude'] = coordinates[i]['coordinates'][1]
        data['longitude'] = coordinates[i]['coordinates'][0]
        message = json.dumps(data)
        messages.append(message)
    return messages

# Function to cyclically send messages for all routes to Kafka
def generate_checkpoint(messages1, messages2, messages3, messages4):
    i1, i2, i3, i4 = 0, 0, 0, 0  # Initialize indices for each route
    # Loop to send messages until the program is stopped
    while True:
        # Simulate sending bus location for each route
        print(messages1[i1])
        print(messages2[i2])
        print(messages3[i3])
        print(messages4[i4])
        # Produce messages to Kafka topic
        producer.produce(messages1[i1].encode('ascii'))
        producer.produce(messages2[i2].encode('ascii'))
        producer.produce(messages3[i3].encode('ascii'))
        producer.produce(messages4[i4].encode('ascii'))
        
        # Wait for half a second before sending the next set of messages
        time.sleep(0.5)

        # Loop back to the start if the end of the message list is reached
        i1 = 0 if i1 == len(messages1)-1 else i1 + 1
        i2 = 0 if i2 == len(messages2)-1 else i2 + 1
        i3 = 0 if i3 == len(messages3)-1 else i3 + 1
        i4 = 0 if i4 == len(messages4)-1 else i4 + 1

# Construct messages from coordinates for each route
messages1 = construct_messages(coordinates1)
messages2 = construct_messages(coordinates2)
messages3 = construct_messages(coordinates3)
messages4 = construct_messages(coordinates4)

# Begin sending messages to Kafka
generate_checkpoint(messages1, messages2, messages3, messages4)
