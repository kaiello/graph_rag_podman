#!/bin/bash

# Usage: ./scripts/run.sh <function_name> <path_to_event.json>
# Example: ./scripts/run.sh splitter events/test_s3_upload.json

FUNC_NAME=$1
EVENT_FILE=$2

# Validation
if [ -z "$FUNC_NAME" ] || [ -z "$EVENT_FILE" ]; then
    echo "Usage: $0 <function_name> <path_to_event.json>"
    exit 1
fi

echo "🚀 Starting app2-$FUNC_NAME..."

# 1. Start the container in detached mode (-d) mapping port 9000
# We verify if .env exists to pass AWS creds (useful if your code talks to real S3)
ENV_OPTS=""
if [ -f .env ]; then
    ENV_OPTS="--env-file .env"
fi

CONTAINER_ID=$(podman run -d --rm -p 9000:8080 $ENV_OPTS app2-$FUNC_NAME)

# 2. Wait briefly for the container to boot
sleep 2

# 3. Send the event payload to the container
echo "📨 Sending event from $EVENT_FILE..."
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d @$EVENT_FILE

echo "" # New line for readability
echo "🛑 Stopping container..."

# 4. Kill the container
podman kill $CONTAINER_ID > /dev/null