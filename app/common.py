import json
from kafka import KafkaProducer, KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

LAB_TEST_ORDERS_TOPIC = "lab-test-orders"
LAB_RESULTS_TOPIC = "lab-results"


def create_producer():
    """
    Returns a Kafka producer that sends JSON messages.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def create_consumer(topic, group_id):
    """
    Returns a Kafka consumer subscribed to a given topic, reading JSON messages.
    """
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
