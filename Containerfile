# Use python 3.11 as base, matching docling requirements (generally python>=3.10)
FROM python:3.11-slim

# Install system dependencies required for image processing (OpenCV/Pillow/Docling/Tesseract)
# libgl1-mesa-glx and libglib2.0-0 are common requirements for cv2 and others
# tesseract-ocr and tesseract-ocr-eng are needed for OCR capabilities in Docling/Unstructured
# build-essential and python3-dev are needed for compiling python extensions
RUN apt-get update && apt-get install -y \
    libgl1-mesa-glx \
    libglib2.0-0 \
    tesseract-ocr \
    tesseract-ocr-eng \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ .

# Command to run the application
CMD ["python", "app.py"]
