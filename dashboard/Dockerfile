# dashboard/Dockerfile

# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container at /app
COPY . .

# Make port 8050 available to the world outside this container
# This should match the port mapping in docker-compose.yml
EXPOSE 8050

# Define the command to run the application when the container starts
# Use Gunicorn for a more production-ready server if needed,
# but for simplicity, we'll use the Flask development server directly.
CMD ["python", "app.py"]
