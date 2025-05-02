# spark-processor/Dockerfile

# Use the same base Spark image as the master/worker for consistency.
# This image is used by the spark-job-runner service.
# The bitnami/spark image includes Python and Spark environment.
FROM bitnami/spark:3.4

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file first for layer caching
COPY requirements.txt .

# Install PySpark dependency specified in requirements.txt
# Although Spark is present, this ensures Python bindings are correctly installed via pip
# --no-cache-dir helps keep the image size smaller
RUN pip install --no-cache-dir -r requirements.txt

# Copy the PySpark processing script into the container
COPY process_flares.py .

# No CMD is needed here because the actual command to run (spark-submit)
# is specified in the docker-compose.yml file for the spark-job-runner service.