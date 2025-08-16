#!/bin/bash

# Build script for Flink cluster deployment (with provided scope)

echo "Building the project for cluster deployment (Flink dependencies as provided)..."
mvn clean package -DskipTests

echo ""
echo "Cluster JAR built successfully!"
echo "Submit to Flink cluster with:"
echo "  flink run target/flink-btc-poc-0.1.0-SNAPSHOT.jar"
echo ""
echo "Or copy to cluster and run:"
echo "  \$FLINK_HOME/bin/flink run /path/to/flink-btc-poc-0.1.0-SNAPSHOT.jar"
