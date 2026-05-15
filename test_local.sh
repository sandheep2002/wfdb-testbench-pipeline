#!/bin/bash

# ===== CONFIG =====
IMAGE_NAME="ecg-pipeline"
AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""
AWS_SESSION_TOKEN=""
AWS_REGION="ap-south-1"
FILE_ID="69fb3eed14d989eefbb9af53"

# ===== BUILD =====
echo "Building Docker image..."
docker build -t ${IMAGE_NAME} .

# ===== RUN LOCALLY =====
echo "Starting Lambda container locally..."
docker run --rm -d \
  --name ecg-test \
  -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
  -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
  -e AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN}" \
  -e AWS_DEFAULT_REGION="${AWS_REGION}" \
  -p 9000:8080 \
  ${IMAGE_NAME}

sleep 2

echo "Invoking with fileId: ${FILE_ID}"
curl -s -X POST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d "{\"fileId\": \"${FILE_ID}\"}" | python3 -m json.tool

# ===== CLEANUP =====
echo "Stopping container..."
docker stop ecg-test