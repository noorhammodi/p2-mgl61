from common import create_consumer, LAB_RESULTS_TOPIC


def main():
    consumer = create_consumer(
        topic=LAB_RESULTS_TOPIC,
        group_id="patient-notification-group",
    )

    print("=== Patient Notification Service ===")
    print(f"Listening for lab results on topic '{LAB_RESULTS_TOPIC}'...")

    for msg in consumer:
        result_event = msg.value
        # In real life this might send an SMS or e-mail
        print("\n📱 New lab result notification for patient:")
        print(
            f"[PATIENT VIEW] Hello {result_event['patient_name']}, "
            f"your {result_event['test_type']} test result is: "
            f"{result_event['result']}"
        )


if __name__ == "__main__":
    main()

