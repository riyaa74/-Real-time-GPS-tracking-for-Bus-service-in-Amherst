# Importing necessary libraries for Flask, Kafka, logging, threading, and queuing
from flask import Flask, render_template, Response
from pykafka import KafkaClient
from pykafka.common import OffsetType
import logging
import time
import threading
import queue

# Function to get a Kafka client connected to a specified host
def get_kafka_client():
    return KafkaClient(hosts='127.0.0.1:9092')

app = Flask(__name__)

# Setting up logging to INFO level
logging.basicConfig(level=logging.INFO)
logger = app.logger

# Route for the index page. It renders an HTML template with various variables.
@app.route('/')
@app.route('/index.html')
def index():
    # Variables for configuring the map interface
    PAGE_TITLE = 'Amherst Bus Live Map'
    # URL template and attributes for the map
    MAP_URL_TEMPLATE = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png'
    MAP_ATTRIBUTION = '...'
    # Starting position and zoom settings for the map
    MAP_STARTING_CENTER = [42.36043, -72.52081]
    MAP_STARTING_ZOOM = 12
    MAP_MAX_ZOOM = 18
    # Kafka topic to subscribe to
    KAFKA_TOPIC = 'geodata_stream_topic_123'
    # Rendering the HTML template with local variables
    return render_template('index.html', **locals())

# Route to stream messages from a Kafka topic
@app.route('/topic/<topicname>')
def get_messages(topicname):
    # Connect to Kafka and create a queue for messages
    client = get_kafka_client()
    message_queue = queue.Queue()

    # Function to consume messages from Kafka and put them into the queue
    def consume_messages():
        consumer = client.topics[topicname].get_simple_consumer(
            auto_offset_reset=OffsetType.LATEST,
            reset_offset_on_start=True
        )
        # Consuming messages and adding them to the queue
        for message in consumer:
            if message is not None:
                message_queue.put(message.value.decode())

    # Generator function to yield messages as server-sent events
    def events():
        start_time = time.time()
        message_count = 0
        # Continuously yielding messages as they come from the queue
        while True:
            message = message_queue.get()
            message_count += 1
            yield f'data:{message}\n\n'
            # Logging info every 100 messages
            if message_count % 100 == 0:
                elapsed_time = time.time() - start_time
                if elapsed_time > 0:
                    rate = message_count / elapsed_time
                    logger.info(f"Processed {message_count} messages in {elapsed_time:.2f} seconds (Rate: {rate:.2f} messages/sec)")

    # Starting a separate thread to consume messages from Kafka
    threading.Thread(target=consume_messages, daemon=True).start()

    # Returning the event stream as a response
    return Response(events(), mimetype="text/event-stream")

# Running the Flask application if this script is the main one executed
if __name__ == '__main__':
    app.run(debug=True, port=5001)
