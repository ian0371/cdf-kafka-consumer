import struct  # Add this import at the beginning of your file
from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
from segment import *
from typing import List


# Configuration for the Kafka consumer
conf = {
    "bootstrap.servers": "localhost:9092",  # Kafka broker address
    "group.id": "test2-py",  # Consumer group ID
    "auto.offset.reset": "earliest",  # Start from the earliest message if no offset is committed
    "enable.auto.commit": False,  # Disable automatic offset commits
}

# Subscribe to the topic
topic = "local.klaytn.chaindatafetcher.en-0.tracegroup.v1"
partition = 0
offset = 0
buffer: List[List[Segment]] = []

# Create the Kafka consumer
consumer = Consumer(conf)
consumer.assign([TopicPartition(topic, partition, offset)])

rowcnt = 0

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            print("rowcnt", rowcnt)
            break

        if msg.error():
            # Handle any errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(
                    f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
                )
            elif msg.error():
                raise KafkaException(msg.error())

        rowcnt += 1

finally:
    # Close the consumer on exit
    consumer.close()
