## Plan: Evolve PortfolioAndRiskJob to full P&L and history tracking

This plan outlines incremental edits to update `PortfolioAndRiskJob` and surrounding operators to support FIFO trade matching, realized and unrealized P&L, and clean separation of latest vs. history storage using Fluss.

### Guiding principles
- Source of truth for inventory and realized P&L is fills (`ExecReport`), not accepted orders.
- Keep operator state lean: positions only for open trades; matcher keeps only unmatched lots.
- Upserts for “latest” views, appends for audit/history.
- Key consistently by `accountId|symbol` across operators.

### Step 1 — Normalize inputs and keys
- Ensure we produce `allExecReports = union(execReports, fakeFilledAcceptedOrders)`.
- Normalize keys: every downstream keyed operator uses `accountId + "|" + symbol`.
- Add basic prints/metrics on streams for visibility during rollout.

Acceptance: `PortfolioAndRiskJob` runs and emits `EXEC` consistently; keys are stable.

### Step 2 — PositionUpdater: only open positions + close events
- Behavior changes:
  - Maintain open inventory per account-symbol: `netQty`, `avgPrice`, `lastUpdated`.
  - When `netQty` becomes zero, emit a one-off PositionClose event to a side output and clear state for that symbol.
  - Only emit non-zero positions on the main output (Open Positions Latest).
- Data model:
  - Introduce `PositionClose` domain: accountId, symbol, totalQty, avgPrice, closeTs, optional lifetime stats.
- Sinks:
  - Main output → Open Positions Latest (Fluss upsert).
  - Side output → Position Close History (Fluss append).

Acceptance: state contains only open symbols; closing a position produces a history row; reopening starts fresh.

### Step 3 — FIFO trade matching (realized P&L source of truth)
- Integrate `FIFOTradeMatchingEngine` keyed by `accountId|symbol` over `allExecReports`.
- Emit `TradeMatch` rows for each matched segment (handles partials, multi-lot matches).
- Sink:
  - TradeMatch History (Fluss append).

Acceptance: existing integration test(s) for simple, multi-lot, and multi-symbol FIFO pass; matches appear in history sink.

### Step 4 — Realized P&L aggregation (latest)
- Add `RealizedPnlAggregator` keyed by `accountId|symbol` over `TradeMatch`.
- Emit cumulative realized P&L snapshots per key.
- Sink:
  - Realized P&L Latest (Fluss upsert).

Acceptance: cumulative values match the sum of historical `TradeMatch` rows per key.

### Step 5 — Price source and Unrealized P&L
- Choose price source (short-term: derive `PriceTick` from `ExecReport`; long-term: market data stream).
- Add `UnrealizedPnlCalculator` joining latest `Position` with latest `PriceTick` keyed by `accountId|symbol`.
- Emit `UnrealizedPnl` snapshots.
- Sink:
  - Unrealized P&L Latest (Fluss upsert).

Acceptance: unrealized updates as prices change; zero when no open position.

### Step 6 — Portfolio aggregation
- Extend `PortfolioUpdater` to include:
  - Cash baseline from `AccountPolicy` (or separate funding stream).
  - Exposure from Open Positions Latest (already provided by PositionUpdater).
  - Optionally incorporate Realized and Unrealized P&L for equity.
- Sink:
  - Portfolio Latest (Fluss upsert).

Acceptance: portfolio equity reflects policy cash + exposure ± P&L (based on selected semantics).

### Step 7 — RiskEngine policies
- Keep simple exposure alert initially.
- Optionally add drawdown or margin usage alerts once portfolio includes P&L.
- Sink:
  - Risk Alerts (Fluss append or external channel).

Acceptance: alerts trigger when thresholds are exceeded; no false positives under normal flows.

### Step 8 — Fluss table design and connectors
- Create or confirm Fluss tables:
  - Upsert tables:
    - `open_positions_latest`
    - `realized_pnl_latest`
    - `unrealized_pnl_latest`
    - `portfolio_latest`
  - Append tables:
    - `exec_report_history`
    - `trade_match_history`
    - `position_close_history`
    - `risk_alerts`
- Wire Flink sinks to these tables with correct primary keys for upserts.

Acceptance: all streams write to intended targets; upserts behave as latest views.

### Step 9 — Correctness and resilience
- Idempotence: deduplicate `ExecReport` if upstream retries (orderId + sequence or unique id) to avoid double counting.
- Watermarking: configure event time on `ExecReport` and `PriceTick` where necessary.
- State growth: only open positions and unmatched lots remain in state; implement TTL as guardrail (not correctness).
- Error paths: log unmatched sells/buys; add DLQ or metrics for anomalies.

Acceptance: system is deterministic under replay and restart; state remains bounded by number of open trades.

### Step 10 — Testing strategy
- Unit:
  - PositionUpdater: open/average, partials, close → clear state + emit close event.
  - FIFO matcher: simple, spanning, partial, multi-symbol; shorting later.
  - Realized aggregator: cumulative accuracy under interleaved matches.
  - Unrealized calc: updates on price/position changes.
- Integration:
  - End-to-end `PortfolioAndRiskJob` with synthetic inputs, asserting all sinks receive expected rows.
- Performance:
  - Load test with many accounts/symbols; verify scaling by key, operator latency, and sink throughput.

Acceptance: tests green; performance within targets.

### Step 11 — Rollout and migration
- Add new sinks alongside existing outputs; keep prints for initial visibility.
- Deploy with sampling or a subset of accounts; validate tables in Fluss.
- Cutover dashboards/queries to latest + history tables.
- Remove legacy outputs once validated.

Acceptance: production cutover without data loss; dashboards show consistent values.

### Step 12 — Observability
- Add counters/gauges:
  - Number of open positions (by account/symbol and total)
  - Unmatched lot queue sizes
  - TradeMatch rate and P&L totals
  - Portfolio equity and exposure histograms
- Add warning logs for unmatched sells/buys and unexpected state transitions.

Acceptance: metrics visible in monitoring; alerts configured for anomalies.

### Step 13 — Documentation updates
- Update `README.md` with:
  - New sinks and how to query latest vs. history
  - How unrealized/realized are computed
  - Operational notes (keys, idempotence, TTL)
- Ensure `architecture.md` diagram remains in sync.

Acceptance: contributors can understand and run the full pipeline with clear expectations.

---

### Deliverables checklist
- Position close side output and `PositionClose` domain
- TradeMatch history sink and tests
- Realized P&L latest sink and tests
- Unrealized P&L latest from positions + prices
- Portfolio with exposure and P&L
- Fluss table definitions and sink wiring
- End-to-end tests and basic monitoring


