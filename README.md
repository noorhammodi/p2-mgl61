
# Set up  env.

# Docker
- docker compose up -d
- cd app/ docker build -t kafka-medical-app .

- Lab
```
docker run --rm -it \
  --network p2-mgl61_default \
  kafka-medical-app \
  lab_processor_consumer.py
```

- Doctor

```
docker run --rm -it \
  --network p2-mgl61_default \
  kafka-medical-app \
  doctor_notification_consumer.py
```

- Patient
```
docker run --rm -it \
  --network p2-mgl61_default \
  kafka-medical-app \
  patient_notification_consumer.py
```

- Stimilus
  ```
  docker run --rm -it \
  --network p2-mgl61_default \
  kafka-medical-app \
  doctor_order_producer.py
```
