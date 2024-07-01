import struct
import hashlib
import sys
from confluent_kafka import (
    Consumer,
    Producer,
    KafkaException,
    KafkaError,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic
from segment import Segment
from typing import Dict, List

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
offset = 0
buffer: List[List[Segment]] = []

# Create the Kafka consumer
consumer = Consumer(conf)
consumer.assign([TopicPartition(topic, partition, offset)])

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "message.max.bytes": 2000000,
})

destination_topic = "local.klaytn.chaindatafetcher.en-0.tracegroup2.v1"

check_duplicate: Dict[bytes, bool] = {}

admin_client = AdminClient(
    {"bootstrap.servers": "localhost:9092", "message.max.bytes": 1000000}
)

try:
    fs = admin_client.delete_topics([destination_topic])
    for topic, f in fs.items():
        f.result()  # Wait for the delete operation to complete
        print(f"Topic {destination_topic} deleted")
except Exception as e:
    print(f"Failed to initiate delete topic {destination_topic}: {e}")

fs = admin_client.create_topics(
    [NewTopic(destination_topic, num_partitions=1, replication_factor=1)]
)
for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print(f"Topic {destination_topic} created")
    except Exception as e:
        print(f"Failed to create topic {destination_topic}: {e}")

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
        sha_hash = hashlib.sha256(msg.value()).digest()
        if sha_hash in check_duplicate:
            print(f"[WARN] the message is duplicated [sha_hash: {sha_hash}]")
            continue

        check_duplicate[sha_hash] = True

        try:
            producer.produce(
                destination_topic,
                key=msg.key(),
                value=msg.value(),
                headers=msg.headers(),
            )
            print(f"[INFO] message produced [key: {msg.key()}]")
        except Exception as e:
            print(f"[ERROR] message produce failed [key: {msg.key()} len(val): {len(msg.value())} e: {e}]")
            sys.exit(1)

        producer.flush()
finally:
    # Close the consumer on exit
    consumer.close()
