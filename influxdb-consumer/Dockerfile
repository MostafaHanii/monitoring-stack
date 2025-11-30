FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY influxdb_consumer.py .

CMD ["python", "-u", "influxdb_consumer.py"]
