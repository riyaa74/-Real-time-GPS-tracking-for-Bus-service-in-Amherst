# Import necessary libraries
from flask import Flask, render_template, Response
from pykafka import KafkaClient
from pykafka.common import OffsetType
import logging
import time

# Function to create a Kafka client connected to the specified host
def get_kafka_client():
    return KafkaClient(hosts='127.0.0.1:9092')

# Initialize Flask app
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = app.logger

# Route for the index page
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
    # Render and return the HTML template with the above variables
    return render_template('index.html', **locals())

# Route to stream messages from a Kafka topic
@app.route('/topic/<topicname>')
def get_messages(topicname):
    # Connect to Kafka
    client = get_kafka_client()

    # Function to yield messages as server-sent events
    def events():
        message_count = 0
        start_time = time.time()

        # Consuming messages from the Kafka topic
        for message in client.topics[topicname].get_simple_consumer(
            auto_offset_reset=OffsetType.LATEST,
            reset_offset_on_start=True
        ):
            message_count += 1
            # Yield each message in a format suitable for server-sent events
            yield 'data:{0}\n\n'.format(message.value.decode())

            # Log processing rate every 100 messages
            if message_count % 100 == 0:
                elapsed_time = time.time() - start_time
                if elapsed_time > 0:
                    rate = message_count / elapsed_time
                    logger.info(f"Processed {message_count} messages in {elapsed_time:.2f} seconds (Rate: {rate:.2f} messages/sec)")

    # Return the event stream as a response
    return Response(events(), mimetype="text/event-stream")

# Run the Flask application if this script is the main one executed
if __name__ == '__main__':
    app.run(debug=True, port=5001)
