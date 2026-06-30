FROM python:3.12-slim

WORKDIR /app

# Cài dependency hệ thống cần thiết cho confluent-kafka (librdkafka) khi build wheel
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc build-essential librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY collector.py .

CMD ["python", "collector.py"]