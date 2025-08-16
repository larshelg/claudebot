#!/bin/bash

echo "=== Setting up Fluss Environment ==="


# Check if Fluss coordinator is ready
echo "3. Checking Fluss coordinator status..."
until nc -z localhost 9123 >/dev/null 2>&1; do
    echo "   Waiting for Fluss coordinator..."
    sleep 5
done
echo "   Fluss coordinator is ready!"

# Check if Flink is ready
echo "4. Checking Flink JobManager status..."
until curl -s http://localhost:8081 >/dev/null 2>&1; do
    echo "   Waiting for Flink JobManager..."
    sleep 5
done
echo "   Flink JobManager is ready!"

echo "5. Creating Fluss tables for indicators..."

# Execute SQL to create tables
docker compose exec jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/data/create-indicators-table.sql

echo ""
echo "=== Setup Complete! ==="
echo "Fluss UI: http://localhost:9123"
echo "Flink UI: http://localhost:8081" 
echo "Flink SQL Gateway: http://localhost:8083"
echo ""
echo "To run the indicator job with Fluss sink:"
echo "  ./run-fluss-job.sh"
echo ""
echo "To stop the environment:"
echo "  docker compose down"
