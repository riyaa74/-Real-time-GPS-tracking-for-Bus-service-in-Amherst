# Importing necessary libraries for Flask, Kafka, logging, threading, queueing, and time
from flask import Flask, render_template, Response
from confluent_kafka import Consumer, KafkaException, KafkaError
import logging
import threading
import queue
import time

# Function to create and configure a Kafka consumer
def get_kafka_consumer(topics):
    config = {
        'bootstrap.servers': 'localhost:9092',  # Kafka broker address
        'group.id': 'my-group',                 # Consumer group ID
        'auto.offset.reset': 'earliest',        # Start reading from the earliest message
        'enable.auto.commit': False,            # Manual offset commit
        # Additional configurations can be added here
    }
    consumer = Consumer(config)
    consumer.subscribe(topics)  # Subscribing to specified topics
    return consumer

app = Flask(__name__)

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = app.logger

# Route for the index page
@app.route('/')
@app.route('/index.html')
def index():
    # Variables for map configuration
    PAGE_TITLE = 'Amherst Bus Live Map'
    # Map settings
    MAP_URL_TEMPLATE = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png'
    MAP_ATTRIBUTION = '...'
    MAP_STARTING_CENTER = [42.36043, -72.52081]
    MAP_STARTING_ZOOM = 12
    MAP_MAX_ZOOM = 18
    KAFKA_TOPIC = 'geodata_stream_topic_123'  # Kafka topic
    return render_template('index.html', **locals())

# Route to stream messages from a Kafka topic
@app.route('/topic/<topicname>')
def get_messages(topicname):
    message_queue = queue.Queue(maxsize=100)  # Queue with a maximum size

    # Function to consume messages from Kafka
    def consume_messages():
        consumer = get_kafka_consumer([topicname])
        total_processing_time = 0
        message_count = 0

        try:
            while True:
                msg = consumer.poll(1.0)  # Poll for messages
                if msg is None: continue
                if msg.error():
                    # Handling partition end and other errors
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
                    else:
                        raise KafkaException(msg.error())
                else:
                    start_time = time.time()
                    process_message(msg)  # Process the message
                    consumer.commit(asynchronous=True)  # Committing the message offset
                    processing_time = time.time() - start_time
                    total_processing_time += processing_time
                    message_count += 1

                    # Log processing rate every 100 messages
                    if message_count % 100 == 0:
                        average_processing_time = message_count / total_processing_time
                        logger.info(f"Processed {message_count} messages in {total_processing_time:.2f} seconds (Rate: {average_processing_time:.2f} messages/sec)")

        except Exception as e:
            logger.error(f'Exception in consume_messages: {e}')
        finally:
            consumer.close()

    # Function to simulate message processing
    def process_message(msg):
        time.sleep(0.01)  # Replace with actual message processing logic
        message_queue.put(msg.value().decode('utf-8'))

    # Generator function to yield messages as server-sent events
    def events():
        while True:
            try:
                message = message_queue.get(timeout=1)  # Retrieve message from the queue
                yield f'data:{message}\n\n'
            except queue.Empty:
                continue

    # Starting a separate thread for consuming messages
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

    # Returning the event stream as a response
    return Response(events(), mimetype="text/event-stream")

# Running the Flask application
if __name__ == '__main__':
    app.run(debug=True, port=5001)
