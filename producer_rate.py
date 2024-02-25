from pykafka import KafkaClient
import json
from datetime import datetime
import uuid
import time

# Open and read data from JSON files for different routes
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

# Setup Kafka producer
# Connects to Kafka on the local machine assuming Kafka is running on the default port (9092)
client = KafkaClient(hosts="localhost:9092")
# Assumes that the Kafka topic 'geodata_stream_topic_123' has already been created
topic = client.topics['geodata_stream_topic_123']
producer = topic.get_sync_producer()

# Initialize a dictionary to hold message data
data = {}
data['service'] = 'alpha'

# Function to construct messages from coordinates
def construct_messages(coordinates):
    messages = []
    for i in range(len(coordinates)):
        # Generate a new UUID for each message
        data['key'] = data['service'] + '_' + str(generate_uuid())
        # Get the current datetime in UTC format
        data['datetime'] = str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        # Extract unit, latitude, and longitude from the coordinates
        data['unit'] = coordinates[i]['unit']
        data['latitude'] = coordinates[i]['coordinates'][1]
        data['longitude'] = coordinates[i]['coordinates'][0]
        # Convert the data dictionary to a JSON string
        message = json.dumps(data)
        messages.append(message)
    return messages

# Function to send messages to Kafka and measure throughput
def generate_checkpoint(messages):
    m = len(messages)
    start_time = time.time()
    for i in range(m):
        # Send each message to Kafka
        producer.produce(messages[i].encode('ascii'))
    # Calculate and print the throughput (messages per second)
    print(m / (time.time() - start_time))

# Construct messages for all routes
messages1 = construct_messages(coordinates1)
messages2 = construct_messages(coordinates2)
messages3 = construct_messages(coordinates3)
messages4 = construct_messages(coordinates4)

# Combine all messages and send to Kafka
messages = messages1 + messages2 + messages3 + messages4
generate_checkpoint(messages)
