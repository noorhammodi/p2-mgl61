import time
from datetime import datetime
from common import (
    create_consumer,
    create_producer,
    LAB_TEST_ORDERS_TOPIC,
    LAB_RESULTS_TOPIC,
)


def main():
    consumer = create_consumer(
        topic=LAB_TEST_ORDERS_TOPIC,
        group_id="lab-processor-group",
    )
    producer = create_producer()

    print("=== Lab Processor ===")
    print(f"Listening for orders on topic '{LAB_TEST_ORDERS_TOPIC}'...")

    for msg in consumer:
        order_event = msg.value
        print("\n🧪 New lab order received:")
        print(order_event)

        # Simulate doing the lab test
        print("Processing test in the lab...")
        time.sleep(2)  # fake delay

        result_value = "negative"  # ultra simple fake result
        result_event = {
            "order_id": order_event["order_id"],
            "patient_name": order_event["patient_name"],
            "test_type": order_event["test_type"],
            "result": result_value,
            "resulted_at": datetime.utcnow().isoformat(),
        }

        print(f"Publishing result to topic '{LAB_RESULTS_TOPIC}'...")
        producer.send(LAB_RESULTS_TOPIC, value=result_event)
        producer.flush()

        print("✅ Lab result published!")
        print("Result event:", result_event)


if __name__ == "__main__":
    main()
