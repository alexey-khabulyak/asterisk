#!/usr/bin/env python3
"""
Read AMI events from a Kafka topic published by res_kafka.

Usage:
    python3 kafka_consumer.py
    python3 kafka_consumer.py --topic asterisk-ami-events
    python3 kafka_consumer.py --bootstrap-servers kafka:9092 --from-beginning
"""

import argparse
import os
import signal
import sys

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
DEFAULT_TOPIC = "asterisk-ami-events"
GROUP_ID = "res-kafka-debug-consumer"


def parse_args():
    parser = argparse.ArgumentParser(description="Consume AMI events from Kafka")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", BOOTSTRAP_SERVERS),
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=f"Kafka topic to consume (default: {DEFAULT_TOPIC})",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Read from the earliest available offset",
    )
    return parser.parse_args()


def ensure_topic(bootstrap_servers: str, topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=5)
    if topic in metadata.topics:
        return

    print(f"Topic '{topic}' not found, creating...")
    result = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    for t, future in result.items():
        try:
            future.result()
            print(f"Topic '{t}' created.")
        except Exception as e:
            print(f"ERROR: failed to create topic '{t}': {e}", file=sys.stderr)
            sys.exit(1)


def main():
    args = parse_args()
    topic = args.topic

    ensure_topic(args.bootstrap_servers, topic)
    offset_reset = "earliest" if args.from_beginning else "latest"

    conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": GROUP_ID,
        "auto.offset.reset": offset_reset,
        "enable.auto.commit": True,
    }

    consumer = Consumer(conf)

    stop = False

    def _sigint(sig, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    consumer.subscribe([topic])
    print(f"Subscribed to {topic} on {args.bootstrap_servers} (offset={offset_reset})")
    print("Press Ctrl+C to stop\n")

    try:
        while not stop:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                code = msg.error().code()
                if code == KafkaError._PARTITION_EOF:
                    continue
                if code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print(f"ERROR: topic '{topic}' does not exist. Create it first:")
                    print(f"  kafka-topics.sh --create --bootstrap-server {args.bootstrap_servers} "
                          f"--topic {topic} --partitions 1 --replication-factor 1")
                    stop = True
                    continue
                raise KafkaException(msg.error())

            key = msg.key().decode() if msg.key() else "(no key)"
            value = msg.value().decode(errors="replace") if msg.value() else "(empty)"
            print(f"--- partition={msg.partition()} offset={msg.offset()} key={key}")
            print(value)

    finally:
        consumer.close()
        print("\nConsumer closed.")


if __name__ == "__main__":
    main()
