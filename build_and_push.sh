#!/bin/bash

# ===== CONFIG =====
IMAGE_NAME="ecg-pipeline"
AWS_REGION="us-east-1"
AWS_ACCOUNT_ID="YOUR_AWS_ACCOUNT_ID"       # e.g. 123456789012
ECR_REPO="ecg-pipeline"

ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}"

# ===== BUILD =====
echo "Building Docker image..."
docker build -t ${IMAGE_NAME} .

# ===== LOCAL TEST (optional) =====
# Uncomment to test locally before pushing
# docker run --rm \
#   -e AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY" \
#   -e AWS_SECRET_ACCESS_KEY="YOUR_SECRET_KEY" \
#   -e AWS_DEFAULT_REGION="${AWS_REGION}" \
#   -p 9000:8080 \
#   ${IMAGE_NAME}
#
# Then in another terminal:
# curl -X POST "http://localhost:9000/2015-03-31/functions/function/invocations" \
#   -d '{"fileId": "your-file-id-here"}'

# ===== PUSH TO ECR =====
echo "Logging into ECR..."
aws ecr get-login-password --region ${AWS_REGION} | \
  docker login --username AWS --password-stdin ${ECR_URI}

echo "Creating ECR repo (skip if exists)..."
aws ecr create-repository --repository-name ${ECR_REPO} --region ${AWS_REGION} 2>/dev/null || true

echo "Tagging image..."
docker tag ${IMAGE_NAME}:latest ${ECR_URI}:latest

echo "Pushing to ECR..."
docker push ${ECR_URI}:latest

echo ""
echo "Done. ECR image URI:"
echo "${ECR_URI}:latest"
echo ""
echo "Use this URI when creating/updating your Lambda function."