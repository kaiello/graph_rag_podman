# Use Debian-based Python image (Has Tesseract and GCC in standard repos)
FROM python:3.11-slim

# 1. Install System Dependencies
# - tesseract-ocr: The OCR engine
# - libgl1: Required for OpenCV/PDF rendering
# - build-essential: Includes GCC/G++ for compiling Numpy
# - python3-dev: Header files for Python extensions
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-eng \
    libgl1 \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Set working directory
WORKDIR ${LAMBDA_TASK_ROOT}

# 3. Install AWS Lambda Runtime Interface Client (RIC)
# This enables the standard Debian container to work as a Lambda
RUN pip install --no-cache-dir awslambdaric

# 4. Copy requirements and install dependencies
# We upgrade pip first to ensure binary wheel compatibility
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 5. Set Environment Variables for performance/caching
ENV HF_HOME=/tmp/hf_home
ENV TORCH_HOME=/tmp/torch_home
ENV DOCLING_HOME=/tmp/docling_home

# 6. Copy Application Code
COPY app.py ${LAMBDA_TASK_ROOT}

# 7. Define Entrypoint (Standard for Custom Lambda Images)
ENTRYPOINT [ "python", "-m", "awslambdaric" ]
CMD [ "app.lambda_handler" ]