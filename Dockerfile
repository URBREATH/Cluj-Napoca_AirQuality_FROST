# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies (if needed for compiling libs, usually not for requests/boto3)
# RUN apt-get update && apt-get install -y gcc

# Copy the dependencies file to the working directory
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Environment variables (Defaults, can be overridden in docker-compose)
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "main.py"]