# System Architecture

## High-Level Data Flow

```
          ┌──────────────────┐
          │  Market Feeds    │  (FIX/WS/REST, Kafka, files)
          └────────┬─────────┘
                   │ ticks/bars
                   ▼
          ┌──────────────────┐
          │   Fluss Topics   │
          │  market_data     │  (symbol,time,price,wm)
          └────────┬─────────┘
                   │
                   │               (Job A)
                   ├──► Flink: Indicators ──────────────────────────────────┐
                   │       • SMA/EMA/RSI/VWAP (UDAFs)                       │
                   │       • Writes wide columns for fast reads             │
                   ▼                                                        │
          ┌──────────────────┐                                             │
          │   Fluss Tables   │                                             │
          │  sma_wide        │  (symbol,time,sma5,sma10,sma20,...)         │
          └────────┬─────────┘                                             │
                   │                                                       │
                   │  (Job B1..Bn — one per strategy)                      │
                   ├──► Flink: Strategy_n  ◄─ Delta Join ──┐               │
                   │        • Reads sma_wide                │               │
                   │        • Joins strategy_config         │               │
                   │        • Emits signals/trades          │               │
                   ▼                                         ▼              │
          ┌──────────────────┐                   ┌──────────────────┐       │
          │  Fluss Tables    │                   │  Fluss Tables    │       │
          │  strategy_config │  (per-strategy)   │  trade_signals   │       │
          └────────┬─────────┘                   └────────┬─────────┘       │
                   │                                        │                │
                   │  (Job C)                               │  (Job D)       │
                   ├──► Flink: Portfolio/Risk  ◄────────────┘                │
                   │       • Consumes trade_signals                           │
                   │       • Joins positions/orders (Delta/temporal)         │
                   │       • Updates portfolio & risk                         │
                   ▼                                                          │
          ┌──────────────────┐                                               │
          │   Fluss Tables   │                                               │
          │ positions        │  (keyed by account/symbol)                    │
          │ orders           │  (changelog by order_id)                      │
          │ portfolio        │  (account-level PnL, exposures)               │
          └────────┬─────────┘                                               │
                   │                                                          │
                   └──► (Job E) Flink: Execution Adapter → Broker/Venue (FIX/REST)
                                 • Reads trade_signals + risk gates
                                 • Sends orders; writes acks/fills back to Fluss
```

## Job Components

### Job C – Portfolio / Risk Engine

#### Inputs
- `trade_signals` (from strategy jobs)
- `venue_fills` (acks/fills/errors from Job D)

#### Core Responsibilities

**Order & Position State** (keyed by account+symbol)
- **Orders**: pending/active orders, status, qty, price, venue orderId
- **Positions**: net qty, avg price, realized/unrealized PnL
- **Portfolio**: aggregate exposure, cash balance, risk metrics

**Risk Checks** (streaming operators)
- Pre-trade check when signal arrives: max notional, per-symbol limits, leverage, etc.
- Kill-switch: if risk breached → publish risk_event (and possibly cancel orders)

#### Outputs
- `validated_orders` (signals that passed risk, to be consumed by Job D)
- `risk_events` (breach alerts)
- `portfolio_state` (changelog table in Fluss for external use / dashboards)
- `positions_state` (changelog, keyed by account+symbol)

#### Flink Operators
- `KeyedProcessFunction<(account, symbol)>` for positions & PnL
- Broadcast/side-output for risk checks
- Sinks to Fluss with upsert mode for changelog semantics

### Job D – Execution Adapter

#### Inputs
- `validated_orders` from Job C

#### Core Responsibilities

**Translate Internal Orders → Venue Protocol**
- FIX/REST/gRPC adapter layer
- Track mapping (internal_orderId → venue_orderId)

**Send to Exchange / Broker**
- Asynchronous I/O (Flink AsyncFunction)
- Retry / error handling logic

**Emit Execution Reports**
- `venue_fills` back into Fluss (acks, partial fills, cancels, errors)
- These flow back into Job C to update orders/positions

#### Outputs
- `venue_fills` → Fluss topic/table
- Optional: `execution_metrics` (latency, reject rates, etc)

### Why This Split Is Beneficial

**Isolation of Concerns:**
- Job C = stateful portfolio & risk
- Job D = connectivity & protocol quirks

**Independent Scaling:**
- You can run many Job D adapters (per exchange), one Job C per account

**Fault Tolerance:**
- Flink checkpoints keep Job C's state consistent
- Job D is mostly stateless aside from orderId maps, so restarts are cheap

**Flexibility:**
- Add a new exchange = spin up another Job D, no changes in Job C
- Deploy new strategies = they just emit into trade_signals, Job C decides if risk is OK

## Data Model

### 1. Accounts Table (Static Metadata / Config)

This is more like a dimension table containing account-level info: owner, leverage settings, base currency, risk limits, permissions, etc. It doesn't change every tick, but can be updated (e.g., increase margin, change max exposure). Perfect candidate for temporal/delta join with Job C.

```sql
CREATE TABLE accounts (
  account STRING,
  owner STRING,
  base_currency STRING,
  max_notional DECIMAL(18,8),
  max_leverage DECIMAL(18,8),
  max_orders INT,
  status STRING,  -- ACTIVE, BLOCKED, CLOSED
  last_update TIMESTAMP(3),
  PRIMARY KEY (account) NOT ENFORCED
) WITH (...);
```

### 2. Order Flow Tables

```sql
-- Validated orders (Job C → Job D)
CREATE TABLE validated_orders (
  order_id STRING,
  account STRING,
  symbol STRING,
  side STRING,
  qty DECIMAL(18,8),
  limit_px DECIMAL(18,8),
  ts TIMESTAMP(3)
) WITH (...);

-- Venue execution reports (Job D → Job C)
CREATE TABLE venue_fills (
  order_id STRING,
  venue_order_id STRING,
  account STRING,
  symbol STRING,
  fill_qty DECIMAL(18,8),
  fill_px DECIMAL(18,8),
  status STRING,  -- NEW/ACK/FILLED/PARTIAL/CANCEL/REJECT
  ts TIMESTAMP(3)
) WITH (...);
```

### 3. Portfolio & Risk Tables

```sql
-- Portfolio / positions state (changelog)
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
) WITH (...);

-- Risk events
CREATE TABLE risk_events (
  account STRING,
  symbol STRING,
  rule STRING,
  severity STRING,
  msg STRING,
  ts TIMESTAMP(3)
) WITH (...);
```

### 4. Data Relationships

- **accounts** = config/master data (slow-changing, controlled by ops/CRM)
- **portfolio_state** = derived aggregates from positions + cash
- **positions_state** = per-symbol granular positions

**Key Relationships:**
- 👉 An account owns a portfolio
- 👉 A portfolio is the aggregation of all positions + cash movements
- 👉 Flink Job C maintains the portfolio & positions tables in Fluss, while referencing the accounts table for limits (via broadcast/delta join)