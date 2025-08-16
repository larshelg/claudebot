#!/bin/bash

# Script to run the Multi-Indicator job with Fluss integration

echo "Building the project for cluster execution with Fluss integration..."
mvn clean package -DskipTests

echo ""
echo "Running the Multi-Indicator Job with Fluss sink..."

# Copy the jar to the cluster volume mount
echo "Copying jar to cluster volume..."
cp target/flink-btc-poc-0.1.0-SNAPSHOT.jar data/

# Submit the job to the Flink cluster
echo "Submitting job to Flink cluster..."
docker compose exec jobmanager /opt/flink/bin/flink run -c com.example.flink.MultiIndicatorWithFlussJob /opt/flink/data/flink-btc-poc-0.1.0-SNAPSHOT.jar

echo ""
echo "Job completed!"

echo ""
echo "To check the data in Fluss, you can use the Flink SQL client:"
echo "  docker compose exec jobmanager /opt/flink/bin/sql-client.sh"
echo ""
echo "Then run queries like:"
echo "  SELECT * FROM fluss_catalog.\`fluss\`.indicators;"
echo "  SELECT * FROM fluss_catalog.\`fluss\`.indicators_history;"
