import struct
from confluent_kafka import Consumer, KafkaException, KafkaError, \
    TopicPartition
from segment import Segment, insert_segment, handle_buffered_messages
from typing import List
import json

# Configuration for the Kafka consumer
conf = {
    "bootstrap.servers": "localhost:9092",  # Kafka broker address
    "group.id": "test2-py",  # Consumer group ID
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # Disable automatic offset commits
}

# Subscribe to the topic
topic = "local.klaytn.chaindatafetcher.en-0.tracegroup.v1"
partition = 0
offset = 6046
buffer: List[List[Segment]] = []

# Create the Kafka consumer
consumer = Consumer(conf)
consumer.assign([TopicPartition(topic, partition, offset)])

try:
    while True:
        # Poll for messages with a timeout of 1 second
        msg = consumer.poll(1.0)

        if msg is None:
            continue  # No message to process

        if msg.error():
            # Handle any errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(
                    f"End of partition reached {msg.topic()} "
                    f"[{msg.partition()}] "
                    f"at offset {msg.offset()}"
                )
            elif msg.error():
                raise KafkaException(msg.error())

        headers = dict(msg.headers())
        totalSegments: int = struct.unpack(">Q", headers["totalSegments"])[0]
        segmentIdx: int = struct.unpack(">Q", headers["segmentIdx"])[0]
        segment = Segment(
            int(msg.key().decode("ascii")),
            msg.value(),
            totalSegments,
            segmentIdx,
            headers["producerId"].decode("ascii"),
        )

        buffer = insert_segment(segment, buffer)
        assembled_data_list = handle_buffered_messages(buffer)

        for assembled_data in assembled_data_list:
            trace_json: dict = json.loads(assembled_data.trace)
            assert trace_json["blockNumber"] == assembled_data.block_number
            assert isinstance(trace_json, dict)
            print(
                assembled_data.block_number,
                len(assembled_data.segments),
                len(assembled_data.trace),
            )

        # Manually commit the message offset
        consumer.commit(msg)
finally:
    # Close the consumer on exit
    consumer.close()
