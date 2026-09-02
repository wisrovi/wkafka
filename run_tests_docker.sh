#!/usr/bin/env bash
set -e

echo "🐳 Running WKafka unit tests inside Docker container (python:3.13)..."

docker run --rm \
  -v "$(pwd):/app" \
  -w /app \
  python:3.13-slim \
  sh -c "apt-get update && apt-get install -y ffmpeg libsm6 libxext6 && pip install --upgrade pip && pip install -e .[snappy] pytest pytest-cov pytest-asyncio && pytest --cov=wkafka tests/"

echo "✅ Docker tests completed successfully."
