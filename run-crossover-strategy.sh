#!/bin/bash

# Script to run the Crossover Strategy job with Fluss integration

echo "Building the crossover strategy project..."
mvn clean package -DskipTests

echo ""
echo "Running the Crossover Strategy Job..."

# Copy the jar to the cluster volume mount
echo "Copying jar to cluster volume..."
cp target/flink-btc-poc-0.1.0-SNAPSHOT.jar data/

# Submit the crossover strategy job to the Flink cluster
echo "Submitting crossover strategy job to Flink cluster..."
docker compose exec jobmanager /opt/flink/bin/flink run -c com.example.flink.CrossoverStrategyJob /opt/flink/data/flink-btc-poc-0.1.0-SNAPSHOT.jar

echo ""
echo "Crossover strategy job submitted!"

echo ""
echo "To check the trading signals in Fluss, you can use the Flink SQL client:"
echo "  docker compose exec jobmanager /opt/flink/bin/sql-client.sh"
echo ""
echo "Then run queries like:"
echo "  SELECT * FROM fluss_catalog.\`fluss\`.strategy_signals ORDER BY \`time\` DESC;"
echo "  SELECT signal, COUNT(*) FROM fluss_catalog.\`fluss\`.strategy_signals GROUP BY signal;"
