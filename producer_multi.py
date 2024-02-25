from pykafka import KafkaClient
import json
from datetime import datetime
import uuid
import time
import threading

# Function to generate a unique identifier
def generate_uuid():
    return uuid.uuid4()

# Establishing a Kafka client and defining a topic
client = KafkaClient(hosts="localhost:9092")
topic = client.topics['geodata_stream_topic_123']
producer = topic.get_sync_producer()

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

# Function to send messages to Kafka and collect statistics
def produce_messages(topic, messages, stats):
    producer = topic.get_producer()
    start_time = time.time()
    for message in messages:
        producer.produce(message.encode('ascii'))
    elapsed_time = time.time() - start_time
    stats.append((len(messages), elapsed_time))

# Main function to orchestrate the message production process
def main():
    client = KafkaClient(hosts="localhost:9092")
    topic = client.topics['geodata_stream_topic_123']

    # List of files containing route data
    route_files = ['./data/amherst/Route1_30.json', './data/amherst/Route2_31.json','./data/amherst/Route3_38.json', './data/amherst/Route4_33.json']

    threads = []  # List to store threads
    stats = []    # List to collect statistics from each thread

    # Creating threads for each route file
    for route_file in route_files:
        with open(route_file) as input_file:
            json_array = json.load(input_file)
            coordinates = json_array['data']
            messages = construct_messages(coordinates)
            thread = threading.Thread(target=produce_messages, args=(topic, messages, stats))
            threads.append(thread)
            thread.start()

    # Waiting for all threads to finish
    for thread in threads:
        thread.join()

    # Calculating total messages sent and total time taken
    total_messages = sum(msg_count for msg_count, _ in stats)
    total_time = sum(time_taken for _, time_taken in stats)
    if total_time > 0:
        ingestion_rate = total_messages / total_time
        print(f"Total messages sent: {total_messages}")
        print(f"Total time taken: {total_time:.2f} seconds")
        print(f"Data ingestion rate: {ingestion_rate:.2f} messages/second")
    else:
        print("No messages were sent or total time taken was too short to calculate rate.")

if __name__ == "__main__":
    main()
