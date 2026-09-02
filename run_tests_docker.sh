#!/usr/bin/env bash
set -e

echo "🐳 Running WKafka unit tests inside Docker container (python:3.13)..."

docker run --rm \
  -v "$(pwd):/app" \
  -w /app \
  python:3.13-slim \
  sh -c "apt-get update && apt-get install -y libgl1 libx11-xcb1 libglib2.0-0 && pip install --upgrade pip && pip install pydantic pytest pytest-cov pytest-asyncio -e .[snappy] && pytest --cov=wkafka tests/"

echo "✅ Docker tests completed successfully."
