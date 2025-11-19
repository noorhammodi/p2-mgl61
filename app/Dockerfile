FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Allow overriding the script via docker run
ENTRYPOINT ["python", "-u"]
CMD ["lab_processor_consumer.py"]
