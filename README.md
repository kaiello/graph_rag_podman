# Podman/S3 Application

This repository contains an application that processes files from an S3 bucket using Podman containers.

## Prerequisites

*   Install [Podman](https://podman.io/).
*   AWS Credentials with access to the specified S3 buckets.

## Setup

1.  Copy the example environment file to create your local configuration:
    ```bash
    cp .env.example .env
    ```
2.  Open `.env` and paste in your specific AWS credentials and configuration:
    ```bash
    AWS_ACCESS_KEY_ID=your_access_key
    AWS_SECRET_ACCESS_KEY=your_secret_key
    AWS_REGION=us-east-1
    S3_BUCKET_NAME=your_input_bucket_name
    ```

## Usage

1.  **Build the container image:**
    Run the build script to create the `s3-app` image.
    ```bash
    ./scripts/build.sh
    ```

2.  **Run the application:**
    Run the container using the run script. This will inject your `.env` variables into the container.
    ```bash
    ./scripts/run.sh
    ```

## Repository Structure

*   `src/`: Contains source code and requirements.
*   `scripts/`: Contains helper scripts for building and running the container.
*   `Containerfile`: Definition of the container image.
*   `test_data/`: Sample data (if applicable).
