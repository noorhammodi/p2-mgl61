from common import create_consumer, LAB_RESULTS_TOPIC


def main():
    consumer = create_consumer(
        topic=LAB_RESULTS_TOPIC,
        group_id="doctor-notification-group",
    )

    print("=== Doctor Notification Service ===")
    print(f"Listening for lab results on topic '{LAB_RESULTS_TOPIC}'...")

    for msg in consumer:
        result_event = msg.value
        print("\n📥 New lab result for doctor dashboard:")
        print(
            f"[DOCTOR VIEW] Patient: {result_event['patient_name']} | "
            f"Test: {result_event['test_type']} | "
            f"Result: {result_event['result']}"
        )


if __name__ == "__main__":
    main()
