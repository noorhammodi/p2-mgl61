
# Set up  env.

- python3.11 -m venv .venv-kafka
- source .venv-kafka/bin/activate
- pip install kafka-python==2.0.2 flask==3.0.0


- cd app
- python lab_processor_consumer.py
- python doctor_notification_consumer.py
- python patient_notification_consumer.py
