## Trading App Indicator Pipeline (Apache Flink + Apache Fluss)

### Overview
This project implements a dual‑mode market data indicator pipeline using:
- Apache Flink for event‑time stream and batch processing
- Apache Fluss for persistent streaming storage

It computes SMA(20) from 1‑minute candles and writes results to:
- `fluss_catalog.fluss.market_state_latest` — latest indicator values per symbol (upsert)
- `fluss_catalog.fluss.indicators_snapshot` — full historical indicator records (append)

Modes:
- Batch mode for historical backfills (no watermark waits)
- Streaming mode for real‑time processing (with watermark handling)

### Repository structure
- `docker-compose.yml` — Flink (with SQL Gateway) + Fluss cluster
- `data/marketdata.sql` — load candles into Fluss and set up stream view
- `data/indicators.sql` — legacy wide-table indicators example (SMA20)
- `data/indicators_kv_setup.sql` — dynamic indicators KV framework setup
- `data/indicator_sma20.sql` — per-indicator job writing SMA(20) to KV snapshot
- `data/indicator_template.sql` — template for adding new indicators
- `data/catalog-store/fluss_catalog.yaml` — Fluss catalog config for Flink

## Quickstart

### 1) Start the stack
```bash
docker compose up -d
```

Flink UI: `http://localhost:8081`  •  Flink SQL Gateway: `http://localhost:8083`

### 2) Open the Flink SQL Client
```bash
docker compose exec jobmanager /opt/flink/bin/sql-client.sh
```

### 3) Load sample market data into Fluss
The compose file mounts `./data` to `/opt/flink/data` in the containers. Adjust the JSON file path in `data/marketdata.sql` if needed, then run:

```sql
SOURCE '/opt/flink/data/marketdata.sql';
```

This creates `fluss_catalog.fluss.market_data_history` and imports candles, plus a streaming view over them.

### 4) Run the indicator pipeline (batch or streaming)
Choose a mode by editing the first lines of `data/indicators.sql`:
- Set `job_mode` to `'batch'` for backfill, or `'streaming'` for live updates.

Then execute:
```sql
SOURCE '/opt/flink/data/indicators.sql';
```

### 5) Verify outputs
```sql
-- Watermark progression (NULL in batch mode)
SELECT * FROM watermark_debug;

-- Latest per‑symbol indicators (upsert sink)
SELECT * FROM fluss_catalog.fluss.market_state_latest;

-- Full historical snapshot (append sink)
SELECT * FROM fluss_catalog.fluss.indicators_snapshot;
```

## Background: Watermarks and OVER windows

In Flink SQL, watermarks control when event‑time windows and `OVER` clauses emit results. If the watermark does not advance beyond a row’s event time, computations can stall (especially with out‑of‑order events or when reading from persistent storage like Fluss).

SMA(20) is computed with an `OVER` window:

```sql
CAST(AVG(`close`) OVER (
  PARTITION BY symbol
  ORDER BY `time`
  ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
) AS DECIMAL(18,8)) AS sma_20
```

In streaming mode you can tune idleness detection:

```sql
SET 'table.exec.source.idle-timeout' = '5 s';
```

## Troubleshooting
- Ensure the JSON file exists in `./data` and the path in `data/marketdata.sql` matches `/opt/flink/data/<file>.json` inside containers.
- If you see no streaming results, check watermark progression via `SELECT * FROM watermark_debug;` and adjust `table.exec.source.idle-timeout`.
- Use the Flink UI at `http://localhost:8081` to inspect running jobs, logs, and task metrics.

## Local Development and Testing

### Testing Indicator Logic
To test the indicator calculations without running the full Flink cluster:

```bash
mvn compile exec:java -Dexec.mainClass="com.example.flink.SimpleIndicatorTest"
```

This will run unit tests for:
- Simple Moving Average (SMA) calculations
- Exponential Moving Average (EMA) calculations  
- Relative Strength Index (RSI) calculations
- Integrated state management

### Running Local Flink Job
To run the full multi-indicator processing job locally:

```bash
./run-local.sh
```

Or manually:
```bash
mvn clean package -DskipTests -Plocal
java --add-opens java.base/java.util=ALL-UNNAMED \
     --add-opens java.base/java.lang=ALL-UNNAMED \
     --add-opens java.base/java.io=ALL-UNNAMED \
     --add-opens java.base/java.time=ALL-UNNAMED \
     -cp target/flink-btc-poc-0.1.0-SNAPSHOT.jar \
     com.example.flink.MultiIndicatorJob
```

The local job generates sample BTC price data and processes it through the indicator pipeline, outputting results to the console.

### Building for Cluster Deployment
To build a JAR for deployment to a Flink cluster:

```bash
./build-cluster.sh
```

Or manually:
```bash
mvn clean package -DskipTests
```

This creates a smaller JAR (~67MB vs ~180MB) with Flink dependencies marked as `provided`, suitable for cluster deployment:

```bash
flink run target/flink-btc-poc-0.1.0-SNAPSHOT.jar
```

### Integration with Fluss
This project includes integration with [Apache Fluss](https://fluss.apache.org/docs/engine-flink/datastream/) for persistent storage of indicator data.

#### Setup Fluss Environment
```bash
./setup-fluss.sh
```

This script will:
- Start Fluss and Flink clusters via Docker Compose
- Create the necessary tables in Fluss:
  - `indicators` - Primary key table for latest indicator values per symbol
  - `indicators_history` - Log table for historical indicator data

#### Run Job with Fluss Integration
```bash
./run-fluss-job.sh
```

This runs the `MultiIndicatorWithFlussJob` which:
1. Generates sample BTC price data
2. Calculates technical indicators (SMA, EMA, RSI)
3. Writes results to Fluss tables for persistence

#### Query Results
Connect to the Flink SQL client to query stored indicators:
```bash
docker compose exec jobmanager /opt/flink/bin/sql-client.sh
```

Example queries:
```sql
-- View latest indicators per symbol
SELECT * FROM fluss_catalog.`fluss`.indicators;

-- View historical indicator data
SELECT * FROM fluss_catalog.`fluss`.indicators_history;

-- Get latest RSI values
SELECT symbol, timestamp, rsi14 
FROM fluss_catalog.`fluss`.indicators 
WHERE rsi14 > 70;  -- Overbought conditions
```

#### Available Services
- **Fluss Coordinator**: `localhost:9123`
- **Flink Web UI**: `http://localhost:8081`
- **Flink SQL Gateway**: `http://localhost:8083`

## Next steps
- Add EMA(20) and RSI(14) calculations
- Parameterize symbols to process selected assets only
- Integrate with a live strategy execution engine

## Dynamic indicators (KV framework)

To add new indicators without recomputing existing ones, use the KV snapshot design:

1) One-time setup (creates normalized table and helper views):
```sql
SOURCE '/opt/flink/data/indicators_kv_setup.sql';
```

2) Run per-indicator jobs independently. Example SMA(20):
```sql
SOURCE '/opt/flink/data/indicator_sma20.sql';
```

3) Add a new indicator by duplicating the template and adjusting logic:
```sql
-- duplicate in repo, then inside SQL client:
SOURCE '/opt/flink/data/indicator_template.sql';
```

Outputs:
- Tall table: `fluss_catalog.fluss.indicators_kv_snapshot` (append-only)
- Latest KV view: `default_catalog.default_database.indicators_kv_latest`
- Optional wide latest view: `default_catalog.default_database.market_state_latest_dynamic`

Notes:
- Each indicator job writes rows tagged with `run_id` (use `'live'` or a unique backfill id).
- Start/stop indicator jobs independently; existing data remains intact.


docker compose exec jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/data/marketdata.sql