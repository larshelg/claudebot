#!/bin/bash

# Simple script to run the Flink job locally with proper JVM arguments

echo "Building the project for local execution (with Flink runtime included)..."
mvn clean package -DskipTests -Plocal

echo ""
echo "Running the Multi-Indicator Job with embedded Flink runtime..."

# Run with JVM arguments to handle Java module system restrictions
java \
  --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.io=ALL-UNNAMED \
  --add-opens java.base/java.time=ALL-UNNAMED \
  -cp target/flink-btc-poc-0.1.0-SNAPSHOT.jar \
  com.example.flink.MultiIndicatorJob

echo ""
echo "Job completed!"
