#!/usr/bin/env bash
set -e

echo "📊 Calculating code coverage for WKafka..."
pytest --cov=wkafka --cov-report=term-missing --cov-report=html tests/
echo "✅ Coverage report generated successfully."
