# P&L Tracking Architecture

This document outlines the complete architecture for real-time P&L tracking and trade analysis in our trading system.

## 🎯 System Overview

### Current State
- **Main Trading Pipeline**: TradeSignals → Risk Checks → Executions → Positions → Portfolio
- **Position Tracking**: In-memory Flink state with basic position management
- **P&L Calculation**: None (removed unrealizedPnl/realizedPnl from Position object)

### Target Architecture
- **Enhanced Trading Pipeline**: Same core flow + ExecReport logging to Fluss
- **Trade Matching Engine**: Separate Flink job for P&L attribution using FIFO matching
- **Dynamic P&L Analysis**: Dashboard queries directly on trade_matches table
- **Complete Audit Trail**: Every P&L dollar traceable to specific buy/sell pairs

---

## 📊 Data Schema Design

### 1. ExecReports Table (Fluss)
**Purpose:** Raw execution data from broker, logged from main trading pipeline

```sql
TABLE exec_reports (
    account_id STRING,
    order_id STRING,        -- Links to original order
    symbol STRING,
    fill_qty DOUBLE,        -- Positive=buy, negative=sell
    fill_price DOUBLE,
    status STRING,          -- FILLED
    timestamp BIGINT,
    
    -- Partitioning & Indexing
    PRIMARY KEY (account_id, symbol, timestamp),
    PARTITION BY RANGE (timestamp)  -- Daily partitions
)
```

**Data Source:** PortfolioAndRiskJob → ExecReport stream → Fluss Sink

### 2. TradeMatches Table (Fluss)  
**Purpose:** Individual P&L calculations with buy/sell attribution

```sql
TABLE trade_matches (
    match_id STRING,        -- UUID for unique matching record
    account_id STRING,
    symbol STRING,
    buy_order_id STRING,    -- Reference to buy ExecReport
    sell_order_id STRING,   -- Reference to sell ExecReport
    matched_qty DOUBLE,     -- Quantity matched between buy/sell
    buy_price DOUBLE,       -- Price from buy execution
    sell_price DOUBLE,      -- Price from sell execution
    realized_pnl DOUBLE,    -- (sell_price - buy_price) * matched_qty
    match_timestamp BIGINT, -- When the match was created
    buy_timestamp BIGINT,   -- Original buy execution time
    sell_timestamp BIGINT,  -- Original sell execution time
    
    -- Partitioning & Indexing
    PRIMARY KEY (account_id, symbol, match_timestamp),
    INDEX (buy_order_id, sell_order_id),
    PARTITION BY RANGE (match_timestamp)  -- Daily partitions
)
```

**Data Source:** TradeMatchingEngine → TradeMatch stream → Fluss Sink

### 3. Position Events Table (Optional)
**Purpose:** Position lifecycle tracking for advanced analytics

```sql
TABLE position_events (
    account_id STRING,
    symbol STRING,
    order_id STRING,
    event_type STRING,      -- OPEN/INCREASE/DECREASE/CLOSE
    net_qty DOUBLE,         -- Position after this trade
    avg_price DOUBLE,       -- Cost basis after this trade
    fill_qty DOUBLE,        -- This execution quantity
    fill_price DOUBLE,      -- This execution price
    timestamp BIGINT,
    
    PRIMARY KEY (account_id, symbol, timestamp)
)
```

---

## 🚀 Flink Jobs Architecture

### Job 1: Enhanced Trading Pipeline
**Existing:** PortfolioAndRiskJob
**Enhancement:** Add ExecReport sink to Fluss

```
Input Streams:
├── TradeSignals (market signals)
├── AccountPolicy (capital & risk limits)
└── ExecReports (broker executions)

Processing Pipeline:
TradeSignals → PreTradeRiskCheck → AcceptedOrders
AcceptedOrders → FakeFill → SimulatedExecReports  
[ExecReports + SimulatedExecReports] → PositionUpdater → Positions
Positions + AccountPolicy → PortfolioUpdater → Portfolio
Portfolio → RiskEngine → RiskAlerts

Output Sinks:
├── Positions → Test/Production Sinks
├── Portfolio → Test/Production Sinks  
├── RiskAlerts → Test/Production Sinks
└── [ExecReports + SimulatedExecReports] → Fluss (exec_reports table)
```

**Key Changes:**
- Add Fluss sink for all ExecReports (real + simulated)
- Maintain existing position/portfolio logic unchanged
- Enable trade matching downstream

### Job 2: Trade Matching Engine (New)
**Purpose:** Calculate realized P&L using FIFO trade matching

```java
public class TradeMatchingJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Read ExecReports from Fluss
        DataStream<ExecReport> execReports = env
            .addSource(new FlussSource<>("exec_reports"))
            .assignTimestampsAndWatermarks(/* event time strategy */);
        
        // Process trade matching
        DataStream<TradeMatch> matches = execReports
            .keyBy(exec -> exec.accountId + "|" + exec.symbol)
            .process(new FIFOTradeMatchingEngine());
        
        // Sink to Fluss
        matches.sinkTo(new FlussSink<>("trade_matches"));
        
        env.execute("Trade Matching Engine");
    }
}
```

**FIFO Matching Algorithm:**
```java
public class FIFOTradeMatchingEngine extends KeyedProcessFunction<String, ExecReport, TradeMatch> {
    
    // State: Queue of unmatched buy executions
    private ListState<ExecReport> unmatchedBuys;
    
    public void processElement(ExecReport exec, Context ctx, Collector<TradeMatch> out) {
        if (exec.fillQty > 0) {
            // Buy order - add to queue
            unmatchedBuys.add(exec);
        } else if (exec.fillQty < 0) {
            // Sell order - match against oldest buys
            matchSellAgainstBuys(exec, out);
        }
    }
    
    private void matchSellAgainstBuys(ExecReport sell, Collector<TradeMatch> out) {
        // FIFO matching logic with partial fills
        // Create TradeMatch records with realized P&L
        // Update unmatchedBuys state
    }
}
```

### Job 3: P&L Analytics (Future)
**Purpose:** Real-time unrealized P&L and advanced analytics

```java
public class PnLAnalyticsJob {
    // Input: TradeMatches (realized P&L) + MarketData (current prices) + PositionEvents
    // Processing: Calculate unrealized P&L, portfolio metrics, risk analytics
    // Output: Real-time dashboard feeds, alerts, reports
}
```

---

## 🔄 Data Flow Diagrams

### End-to-End P&L Flow
```
[Broker Executions] 
    ↓
[Main Trading Pipeline]
    ↓
[ExecReports] → Fluss (exec_reports)
    ↓
[Trade Matching Engine]
    ↓
[TradeMatches] → Fluss (trade_matches)
    ↓
[P&L Dashboard Queries]
```

### Real-time Processing Flow
```
Event Time: T+0    T+1    T+2    T+3
            │      │      │      │
ExecReport: BUY    BUY    SELL   SELL
            │      │      │      │
Fluss:     [────── Append Only ──────]
            │      │      │      │
Matching:   -      -    Match1  Match2
            │      │      │      │
P&L:        -      -     $100   $150
```

### State Management
```
Trading Job State (Flink):
├── Position State (per account-symbol)
├── Portfolio State (per account)
└── Risk State (per account)

Matching Job State (Flink):
└── Unmatched Buys Queue (per account-symbol)

Historical Data (Fluss):
├── exec_reports (all executions)
└── trade_matches (all P&L records)
```

---

## 📈 P&L Analytics Architecture

### Dual Query Strategy with Fluss Lakehouse

**Real-time Queries (Hot Path):**
- Query Fluss directly for live dashboard updates
- Recent P&L, current positions, real-time alerts
- Low latency, small time windows (last hour/day)

**Analytics Queries (Cold Path):**
- Query lakehouse layer for historical analysis
- Aggregated reports, trend analysis, backtesting
- High throughput, large time windows (weeks/months/years)

### Real-time Dashboard Queries (Fluss)

#### 1. Live Account P&L (Last 24 Hours)
```sql
-- Real-time total P&L for live monitoring
SELECT 
    account_id,
    SUM(realized_pnl) as today_realized_pnl,
    COUNT(*) as today_trades,
    SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as today_win_rate
FROM trade_matches 
WHERE account_id = ? AND match_timestamp > UNIX_TIMESTAMP(NOW() - INTERVAL 1 DAY) * 1000
GROUP BY account_id;
```

#### 2. Recent Trade Activity
```sql
-- Last 10 trades for real-time feed
SELECT 
    symbol, matched_qty, buy_price, sell_price, realized_pnl,
    FROM_UNIXTIME(match_timestamp / 1000) as trade_time
FROM trade_matches 
WHERE account_id = ?
ORDER BY match_timestamp DESC 
LIMIT 10;
```

### Historical Analytics Queries (Lakehouse)

#### 1. Account Total P&L (All Time)
```sql
-- Historical performance analysis
SELECT 
    account_id,
    SUM(realized_pnl) as total_realized_pnl,
    COUNT(*) as total_trades,
    AVG(realized_pnl) as avg_pnl_per_trade,
    SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
    MAX(realized_pnl) as best_trade,
    MIN(realized_pnl) as worst_trade
FROM trade_matches_lakehouse 
WHERE account_id = ?
GROUP BY account_id;
```

#### 2. P&L by Symbol (Historical)
```sql
-- Symbol performance ranking
SELECT 
    symbol,
    SUM(realized_pnl) as symbol_pnl,
    COUNT(*) as trade_count,
    AVG(realized_pnl) as avg_pnl_per_trade,
    STDDEV(realized_pnl) as pnl_volatility
FROM trade_matches_lakehouse 
WHERE account_id = ?
GROUP BY symbol
ORDER BY symbol_pnl DESC;
```

#### 3. P&L Over Time (Daily Aggregation)
```sql
-- Time series analysis for charting
SELECT 
    DATE(FROM_UNIXTIME(match_timestamp / 1000)) as trade_date,
    SUM(realized_pnl) as daily_pnl,
    COUNT(*) as daily_trades,
    SUM(SUM(realized_pnl)) OVER (
        ORDER BY DATE(FROM_UNIXTIME(match_timestamp / 1000))
        ROWS UNBOUNDED PRECEDING
    ) as cumulative_pnl
FROM trade_matches_lakehouse 
WHERE account_id = ? 
  AND match_timestamp >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 90 DAY)) * 1000
GROUP BY DATE(FROM_UNIXTIME(match_timestamp / 1000))
ORDER BY trade_date;
```

#### 4. Advanced Analytics
```sql
-- Monthly performance trends
SELECT 
    YEAR(FROM_UNIXTIME(match_timestamp / 1000)) as year,
    MONTH(FROM_UNIXTIME(match_timestamp / 1000)) as month,
    SUM(realized_pnl) as monthly_pnl,
    COUNT(*) as monthly_trades,
    COUNT(DISTINCT symbol) as symbols_traded
FROM trade_matches_lakehouse 
WHERE account_id = ?
GROUP BY YEAR(FROM_UNIXTIME(match_timestamp / 1000)), 
         MONTH(FROM_UNIXTIME(match_timestamp / 1000))
ORDER BY year DESC, month DESC;

-- Trading activity heatmap
SELECT 
    DAYOFWEEK(FROM_UNIXTIME(match_timestamp / 1000)) as day_of_week,
    HOUR(FROM_UNIXTIME(match_timestamp / 1000)) as hour_of_day,
    COUNT(*) as trade_count,
    AVG(realized_pnl) as avg_pnl
FROM trade_matches_lakehouse 
WHERE account_id = ?
GROUP BY DAYOFWEEK(FROM_UNIXTIME(match_timestamp / 1000)),
         HOUR(FROM_UNIXTIME(match_timestamp / 1000))
ORDER BY day_of_week, hour_of_day;
```

### Query Performance Optimization

**Fluss Layer (Real-time):**
- Partition by `account_id` for fast filtering
- Index on `match_timestamp` for time-based queries
- Keep hot data (last 7-30 days) in memory

**Lakehouse Layer (Analytics):**
- Columnar storage (Parquet/Delta Lake)
- Partition by date (`YYYY/MM/DD`) for time-based filtering
- Pre-aggregated tables for common queries
- Compression and bloom filters for better scan performance

---

## 🏗️ Implementation Roadmap

### Phase 1: Foundation (Week 1-2)
**Goal:** ExecReport logging to Fluss

- [ ] Create TradeMatch domain object
- [ ] Add Fluss dependency to project
- [ ] Configure Fluss connection and table schemas
- [ ] Add ExecReport sink to PortfolioAndRiskJob
- [ ] Test ExecReport logging with integration tests

**Deliverable:** ExecReports flowing to Fluss storage

### Phase 2: Trade Matching (Week 3-4)
**Goal:** P&L calculation and attribution

- [ ] Implement FIFOTradeMatchingEngine
- [ ] Create TradeMatchingJob
- [ ] Add TradeMatch sink to Fluss
- [ ] Test trade matching with various scenarios (partial fills, etc.)
- [ ] Validate P&L calculations manually

**Deliverable:** Complete trade matching with P&L attribution

### Phase 3: Analytics & Dashboard (Week 5-6)
**Goal:** P&L visualization and reporting

- [ ] Implement dashboard query service
- [ ] Create P&L visualization components
- [ ] Add real-time P&L updates
- [ ] Performance testing and optimization
- [ ] User acceptance testing

**Deliverable:** Working P&L dashboard with real-time updates

### Phase 4: Advanced Features (Week 7+)
**Goal:** Enhanced analytics and monitoring

- [ ] Unrealized P&L calculation with market data
- [ ] Advanced risk metrics and alerts
- [ ] Historical replay and backtesting
- [ ] Export capabilities and reporting
- [ ] Performance monitoring and alerting

**Deliverable:** Production-ready P&L tracking system

---

## 🔧 Technical Considerations

### Performance
- **Partitioning:** Both tables partitioned by timestamp for query performance
- **Indexing:** Composite indexes on account_id + symbol + timestamp
- **Retention:** Implement data retention policies for long-term storage

### Scalability
- **Parallel Processing:** Trade matching engine can be scaled per account-symbol
- **State Backend:** Configure RocksDB for large state scenarios
- **Checkpointing:** Enable for fault tolerance

### Monitoring
- **Metrics:** Track matching latency, P&L calculation accuracy, data freshness
- **Alerts:** Failed jobs, data quality issues, unusual P&L patterns
- **Dashboards:** System health, processing rates, storage utilization

### Testing Strategy
- **Unit Tests:** Individual matching scenarios, edge cases
- **Integration Tests:** End-to-end P&L flow validation
- **Performance Tests:** High-volume trade matching
- **Data Quality Tests:** P&L accuracy validation

---

*This architecture provides complete P&L tracking from individual trades to portfolio-level analytics while maintaining clean separation of concerns and scalability.*