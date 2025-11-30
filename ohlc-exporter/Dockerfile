# Start from a small Python image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED 1
ENV EXPORTER_PORT 9001

# Create and set the working directory
WORKDIR /app

# Install dependencies (prometheus-client and a websocket library)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY binance_exporter.py /app/exporter.py

# Expose the port Prometheus will scrape
EXPOSE 9001

# Command to run the exporter
CMD ["python", "exporter.py"]