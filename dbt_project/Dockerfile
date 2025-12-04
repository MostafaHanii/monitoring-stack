FROM python:3.10-slim

WORKDIR /usr/app

# Install dbt-snowflake
RUN pip install dbt-snowflake==1.7.0

# Copy project files
COPY . .

# Overwrite profiles.yml with the Docker version
COPY profiles_docker.yml profiles.yml

# Make script executable
RUN chmod +x run_loop.sh

CMD ["./run_loop.sh"]
