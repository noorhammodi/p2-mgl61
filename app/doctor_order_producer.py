import uuid
import time
from datetime import datetime
from common import create_producer, LAB_TEST_ORDERS_TOPIC


def main():
    producer = create_producer()
    print("=== Doctor Lab Test Order Producer ===")

    patient_name = input("Patient full name: ")
    test_type = input("Test type (e.g. blood, MRI, XRay): ")

    order_id = str(uuid.uuid4())

    event = {
        "order_id": order_id,
        "source": "doctor",
        "patient_name": patient_name,
        "test_type": test_type,
        "priority": "normal",
        "ordered_at": datetime.utcnow().isoformat(),
    }

    print(f"\nPublishing lab test order to topic '{LAB_TEST_ORDERS_TOPIC}'...")
    producer.send(LAB_TEST_ORDERS_TOPIC, value=event)
    producer.flush()

    print("✅ Order sent!")
    print("Event:", event)
    print("\nNow watch the lab and notification consumers for processing.")


if __name__ == "__main__":
    main()

