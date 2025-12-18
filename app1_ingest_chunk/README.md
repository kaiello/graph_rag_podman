# S3 File Processor

This containerized application processes files from S3 using a Python script.

## Setup

1. Copy the example environment file to `.env`:
   ```bash
   cp .env.example .env
   ```
2. Populate `.env` with your AWS credentials and S3 bucket name.

## Build

Build the container image using the provided script:
```bash
./scripts/build.sh
```

## Run

Run the application container:
```bash
./scripts/run.sh
```

## Architecture

This containerized application processes files from S3 using a Python script.
