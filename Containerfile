# Use python 3.11 as base
FROM python:3.11-slim

# Prevent interactive prompts during build
ARG DEBIAN_FRONTEND=noninteractive

# Install system dependencies
# libgl1 replaced libgl1-mesa-glx for Debian Bookworm compatibility
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    tesseract-ocr \
    tesseract-ocr-eng \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ .

# Command to run the application
CMD ["python", "app.py"]
