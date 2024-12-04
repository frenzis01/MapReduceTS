#!/bin/bash

# Check if Kafka is listening on port 9092
if nc -z localhost 9092; then
  # Check if Kafka can respond to a metadata request
  echo "Checking Kafka metadata..."
  kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "Kafka is ready."
    exit 0
  else
    echo "Kafka metadata request failed."
    exit 1
  fi
else
  echo "Kafka is not listening on port 9092."
  exit 1
fi