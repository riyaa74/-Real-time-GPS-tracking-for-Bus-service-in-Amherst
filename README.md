# Real time GPS tracking for Bus service in Amherst

Introduction

This project implements a real-time bus tracking system using Apache Kafka. It allows you to track buses in real-time as they move along routes and provides insights into data ingestion and processing rates.


Setup
Follow these steps to set up and run the project:

1. Download and Setup ZooKeeper
Download ZooKeeper from the official website.
Extract the downloaded archive.
Open a terminal and navigate to the ZooKeeper directory.
Run the following command to start ZooKeeper:
   
     ./bin/zookeeper-server-start.sh config/zookeeper.properties
   

2. Download and Setup Kafka
Download Kafka from the official website.
Extract the downloaded archive.
Open a terminal and navigate to the Kafka directory.
Run the following command to start Kafka:
  
     ./bin/kafka-server-start.sh config/server.properties


3. Run Components

Open four terminal windows:

In the 1st terminal, start ZooKeeper:

     ./bin/zookeeper-server-start.sh config/zookeeper.properties


 In the 2nd terminal, start Kafka:

     ./bin/kafka-server-start.sh config/server.properties


In the 3rd terminal, run the producer script:

     python producer_view.py


In the 4th terminal, run the consumer script:
  
     python consumer_view.py


4. View Bus Tracking

In the consumer terminal, you'll see a link to a port. Copy and paste that link into a web browser.
You will see four buses moving in real-time based on input data.

5. Tests and Experiments analysis
   To analyze data processing rate, run `consumer_rate.py`.
   To analyze data ingestion rate, run `producer_rate.py`.
   There are variations for faster data processing and ingestion rates:
Faster data processing: `consumer_confluent-kafka.py` and `consumer_async-queue.py`
Faster data ingestion: `producer_partition.py`, `producer_batch.py`, and `producer_multi.py`


Conclusion

The project provides a robust real-time bus tracking system using Kafka and offers insights into data ingestion and processing rates. Feel free to explore and optimize various aspects of the system to meet your specific requirements.

