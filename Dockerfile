# Assuming your image is built from the Python base you were using
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy your Python code (exporters) and the startup script
COPY ./exporters /app/ 
COPY start.sh /app/
COPY requirements.txt .
# Make the startup script executable
RUN chmod +x /app/start.sh
RUN pip install --no-cache-dir -r requirements.txt
# Set the ENTRYPOINT to run the startup script
# Using the Exec Form (JSON array) is best practice
ENTRYPOINT ["/app/start.sh"]