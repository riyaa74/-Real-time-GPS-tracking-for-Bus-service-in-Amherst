# Importing necessary libraries for Flask web framework and PyKafka for Kafka integration
from flask import Flask, render_template, Response
from pykafka import KafkaClient
from pykafka.common import OffsetType

# Function to create and return a Kafka client connected to the specified Kafka server
def get_kafka_client():
    return KafkaClient(hosts='127.0.0.1:9092')

# Initializing the Flask application
app = Flask(__name__)

# Route for the main index page
@app.route('/')
@app.route('/index.html')
def index():
    # Setting up variables for map configuration in the web page
    PAGE_TITLE = 'Amherst Bus Live Map'
    MAP_URL_TEMPLATE = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png'
    MAP_ATTRIBUTION = '...'
    MAP_STARTING_CENTER = [42.36043, -72.52081]
    MAP_STARTING_ZOOM = 12
    MAP_MAX_ZOOM = 18
    KAFKA_TOPIC = 'geodata_stream_topic_123'  # Kafka topic to be used
    # Rendering the HTML template with the provided variables
    return render_template('index.html', **locals())

# Route for the Kafka consumer API
@app.route('/topic/<topicname>')
def get_messages(topicname):
    # Establishing connection to Kafka
    client = get_kafka_client()

    # Generator function for streaming Kafka messages
    def events():
        # Setting up a Kafka consumer for the specified topic
        for message in client.topics[topicname].get_simple_consumer(
            auto_offset_reset=OffsetType.LATEST,  # Setting offset to the latest to avoid old data
            reset_offset_on_start=True           # Ensuring offset is reset on start
        ):
            # Yielding each Kafka message in a format suitable for server-sent events
            yield 'data:{0}\n\n'.format(message.value.decode())

    # Returning the event stream as a server-sent events response
    return Response(events(), mimetype="text/event-stream")

# Running the Flask application if this script is executed as the main program
if __name__ == '__main__':
    app.run(debug=True, port=5001)
