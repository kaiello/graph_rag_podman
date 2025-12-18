#!/bin/bash
podman run --env-file .env --name s3-app-instance s3-app
