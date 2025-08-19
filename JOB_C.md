# Job C - Portfolio & Risk Management

## Overview

Job C is the central risk management and portfolio tracking component that acts as the gatekeeper between trading strategies and order execution. It performs risk pre-checks, maintains real-time portfolio state, and provides continuous risk monitoring.

**Core Function**: Risk pre-checks (SQL) + Positions updater + Portfolio updater + Risk monitoring → Fluss tables

## Inputs

### 1. Trade Signals (`trade_signals` from strategies)
- **Purpose**: Proposed trades (BUY/SELL, qty, price, account)
- **Processing**: Joined with accounts table for risk pre-checks
- **Source**: Strategy jobs (Job B1..Bn)

### 2. Execution Reports (`exec_reports` from Job D – Execution Adapter)
- **Purpose**: Actual venue responses (FILLED / PARTIAL / REJECTED)
- **Processing**: Drive real updates to positions & portfolio
- **Source**: Job D (Execution Adapter)

### 3. Accounts Config (`accounts` table in Fluss)
- **Purpose**: Account-level parameters (limits, leverage, base currency, status)
- **Usage**: Pre-trade checks & continuous validation
- **Type**: Slow-changing dimension table

## State Tables Managed

### Positions Table (`positions_state`)
- **Key**: `(account, symbol)`
- **Columns**: `net_qty`, `avg_px`, `unrealized_pnl`, `realized_pnl`, `last_update`
- **Purpose**: Track per-symbol positions for each account

### Portfolio Table (`portfolio_state`)
- **Key**: `(account)`
- **Columns**: `equity`, `cash_balance`, `margin_used`, `total_exposure`, `realized_pnl`, `unrealized_pnl`, `last_update`
- **Purpose**: Account-level aggregated metrics

### Orders Table (`orders_state`) - Optional
- **Key**: `(account, order_id)`
- **Columns**: `symbol`, `qty`, `price`, `status`, `timestamps`
- **Purpose**: Track open/closed orders

## Core Components

### 1. Risk Pre-Check (SQL View)
- **Function**: Joins `trade_signals` with `accounts`
- **Logic**: Filters out signals that violate limits
- **Output**: Passes only ACCEPTED signals forward

```sql
CREATE VIEW validated_signals AS
SELECT 
  s.account,
  s.symbol,
  s.side,
  s.qty,
  s.price,
  CASE
    WHEN a.status <> 'ACTIVE' THEN 'REJECT_ACCOUNT'
    WHEN (s.qty * s.price) > a.max_notional THEN 'REJECT_NOTIONAL'
    ELSE 'ACCEPT'
  END AS decision
FROM trade_signals AS s
JOIN accounts FOR SYSTEM_TIME AS OF s.proc_time AS a
ON s.account = a.account;
```

### 2. PositionUpdater (ProcessFunction)
- **Keying**: `(account, symbol)`
- **Function**: Updates net position, average price, PnL per symbol
- **Output**: Emits updates to `positions_state` in Fluss

### 3. PortfolioUpdater (ProcessFunction)
- **Keying**: `(account)`
- **Function**: Aggregates positions into portfolio metrics (equity, margin, exposure)
- **Output**: Emits updates to `portfolio_state` in Fluss

### 4. Risk Engine / Kill-Switch
- **Function**: Continuously checks portfolio metrics against account limits
- **On Breach**: Publishes `risk_alerts` or `kill_signal` to Fluss
- **Integration**: Consumed by Job D to stop sending orders

## Table Definitions

### Source Tables

#### Trade Signals
```sql
CREATE TABLE trade_signals (
  account STRING,
  symbol STRING,
  side STRING,   -- BUY/SELL
  qty DECIMAL(18,8),
  price DECIMAL(18,8),
  ts TIMESTAMP(3),
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'fluss',
  'table-name' = 'trade_signals'
);
```

#### Accounts Configuration
```sql
CREATE TABLE accounts (
  account STRING,
  base_currency STRING,
  max_notional DECIMAL(18,8),
  max_leverage DECIMAL(18,8),
  max_orders INT,
  status STRING,              -- ACTIVE, BLOCKED, CLOSED
  last_update TIMESTAMP(3),
  PRIMARY KEY (account) NOT ENFORCED
) WITH (
  'connector' = 'fluss',
  'table-name' = 'accounts'
);
```

### Output Tables

#### Portfolio State
```sql
CREATE TABLE portfolio_state (
  account STRING,
  equity DECIMAL(18,8),
  cash_balance DECIMAL(18,8),
  margin_used DECIMAL(18,8),
  unrealized_pnl DECIMAL(18,8),
  realized_pnl DECIMAL(18,8),
  total_exposure DECIMAL(18,8),
  last_update TIMESTAMP(3),
  PRIMARY KEY (account) NOT ENFORCED
) WITH ('connector' = 'fluss');
```

#### Positions State
```sql
CREATE TABLE positions_state (
  account STRING,
  symbol STRING,
  net_qty DECIMAL(18,8),
  avg_px DECIMAL(18,8),
  unrealized_pnl DECIMAL(18,8),
  realized_pnl DECIMAL(18,8),
  last_update TIMESTAMP(3),
  PRIMARY KEY (account, symbol) NOT ENFORCED
) WITH ('connector' = 'fluss');
```

## Implementation Example

### DataStream Job Structure
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// Trade signals (already risk-checked in SQL view)
Table validated = tEnv.from("validated_signals");

// Execution reports stream
Table execReports = tEnv.sqlQuery("""
  SELECT
    account,
    symbol,
    side,
    qty,
    price,
    status,        -- FILLED / REJECTED / PARTIAL
    ts
  FROM exec_reports
""");

DataStream<ExecReport> reports = tEnv.toDataStream(execReports, ExecReport.class);

// Maintain portfolio state keyed by account
reports
  .keyBy(r -> r.account)
  .process(new PortfolioUpdater())
  .sinkTo(Fluss.sink("portfolio_state"));

// Maintain positions state keyed by account+symbol
reports
  .keyBy(r -> r.account + "|" + r.symbol)
  .process(new PositionUpdater())
  .sinkTo(Fluss.sink("positions_state"));
```

### Portfolio Updater Implementation
```java
public class PortfolioUpdater extends KeyedProcessFunction<String, ExecReport, PortfolioState> {

    private ValueState<PortfolioState> state;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<PortfolioState> desc =
            new ValueStateDescriptor<>("portfolio", PortfolioState.class);
        state = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(ExecReport report, Context ctx, Collector<PortfolioState> out) throws Exception {
        PortfolioState ps = state.value();
        if (ps == null) ps = new PortfolioState(report.account);

        if ("FILLED".equals(report.status)) {
            BigDecimal notional = report.price.multiply(report.qty);
            if ("BUY".equals(report.side)) {
                ps.cashBalance = ps.cashBalance.subtract(notional);
                ps.netExposure = ps.netExposure.add(notional);
            } else {
                ps.cashBalance = ps.cashBalance.add(notional);
                ps.netExposure = ps.netExposure.subtract(notional);
            }
            ps.lastUpdate = Instant.now();
        }

        state.update(ps);
        out.collect(ps);
    }
}
```

## Outputs

### Primary Outputs
- **`positions_state`** → Live per-symbol positions
- **`portfolio_state`** → Account-level snapshot
- **`risk_alerts`** → Warnings/triggers when exposure/margin breached
- **`kill_signals`** → Instructions to cancel/reject further trades for an account

### Processing Flow
1. **Trade signal** → Risk pre-check (SQL join with accounts)
2. **If valid** → Forward to Job D & record pending order
3. **Execution report** → Update positions & portfolio via keyed state
4. **Risk monitoring** → Continuous validation against limits
5. **State updates** → Emit to Fluss tables for downstream consumption

## Benefits

✅ **This architecture provides:**

- **Pre-trade Risk Control**: All signals validated before execution
- **Real-time State Management**: Live portfolio and position tracking
- **Continuous Risk Monitoring**: Kill-switch capability for breach scenarios
- **Centralized Configuration**: Account limits managed in single source
- **Audit Trail**: Complete history of positions and portfolio changes
- **Scalable Processing**: Keyed state allows parallel processing per account/symbol
- **Fault Tolerance**: Flink checkpointing ensures state consistency