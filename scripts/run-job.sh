#!/bin/bash

# Enhanced script to run any Flink job with Fluss integration
# Automatically discovers available job classes and lets user select

set -e

# Hardcoded job definitions from jobs folder
job_classes=(
    "com.example.flink.jobs.CrossoverStrategyJob"
    "com.example.flink.jobs.TradingEngineJob"
    "com.example.flink.jobs.LocalExecutionJob"
    "com.example.flink.jobs.StrategyChooserClusterJob"
    "com.example.flink.jobs.MultiIndicatorWithFlussJob"
    "com.example.flink.jobs.LineJob"
)

job_descriptions=(
    "Crossover Strategy - Moving average crossover signals"
    "Trading Engine - Complete trade matching and portfolio management"
    "Local Execution - Local order execution simulation"
    "Strategy Chooser - Multi-strategy signal generation"
    "Multi Indicator - Technical indicators with Fluss integration"
    "LineJob"
)

# Display available jobs
echo "📋 Available Flink Jobs:"
echo ""
for i in "${!job_classes[@]}"; do
    printf "%2d) %-50s %s\n" $((i+1)) "${job_classes[i]}" "${job_descriptions[i]}"
done

echo ""
echo -n "🎯 Select job number (1-${#job_classes[@]}): "
read -r selection

# Validate selection
if ! [[ "$selection" =~ ^[0-9]+$ ]] || [[ $selection -lt 1 ]] || [[ $selection -gt ${#job_classes[@]} ]]; then
    echo "❌ Invalid selection: $selection"
    exit 1
fi

# Get selected job class
selected_index=$((selection - 1))
selected_job="${job_classes[selected_index]}"
selected_description="${job_descriptions[selected_index]}"

echo ""
echo "✅ Selected: $selected_job"
echo "📝 Description: $selected_description"
echo ""

# Ask if user wants to build
echo -n "🔨 Build the project first? (y/n) [y]: "
read -r build_choice

# Default to yes if empty
if [[ -z "$build_choice" ]]; then
    build_choice="y"
fi

if [[ "$build_choice" =~ ^[Yy] ]]; then
    echo "🔨 Building the project..."
    mvn clean package -DskipTests

    if [[ $? -ne 0 ]]; then
        echo "❌ Build failed!"
        exit 1
    fi
    echo "✅ Build completed successfully!"
else
    echo "⏭️  Skipping build step..."

    # Check if JAR exists
    if [[ ! -f "../target/flink-cep-strategy-1.0-SNAPSHOT.jar" ]]; then
        echo "❌ JAR file not found: ../target/flink-cep-strategy-1.0-SNAPSHOT.jar"
        echo "   You need to build the project first or run with build option."
        exit 1
    fi
fi

echo ""
echo "📦 Copying JAR to cluster volume..."
cp ../target/flink-cep-strategy-1.0-SNAPSHOT.jar ../data/

# Submit the job to the Flink cluster
echo ""
echo "🚀 Submitting job to Flink cluster..."
echo "   Class: $selected_job"
echo ""

docker compose exec jobmanager /opt/flink/bin/flink run -d -c "$selected_job" /opt/flink/data/flink-cep-strategy-1.0-SNAPSHOT.jar

if [[ $? -eq 0 ]]; then
    echo ""
    echo "✅ Job submitted successfully!"
    echo ""
    echo "🔍 To monitor the job:"
    echo "   Web UI: http://localhost:8081"
    echo "   SQL Client: docker compose exec jobmanager /opt/flink/bin/sql-client.sh"
    echo ""
    echo "📊 Common queries to check results:"
    echo "   SHOW TABLES;"
    echo "   SELECT * FROM fluss_catalog.\`fluss\`.indicators ORDER BY \`time\` DESC LIMIT 10;"
    echo "   SELECT * FROM fluss_catalog.\`fluss\`.strategy_signals ORDER BY \`time\` DESC LIMIT 10;"
    echo "   SELECT * FROM fluss_catalog.\`fluss\`.trade_signal_history ORDER BY ts DESC LIMIT 10;"
else
    echo "❌ Job submission failed!"
    exit 1
fi
