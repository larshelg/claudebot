# Trading System Snapshot (for future reference)
_Date: 2025‑08‑29 • Timezone: Europe/Oslo_

This snapshot captures architecture, data contracts, policies, and ops knobs for the current trading platform. It’s designed so you can paste it back to me later and we’ll be perfectly aligned without rehashing prior chats.

---

## 1) High‑level architecture
**Dataflow (prod):**
1) **Prices** → `price_1m_history` (Fluss, append)
2) **Indicators Job** → `indicator_history` (append) + `indicator_latest` (upsert)
3) **Strategies (N jobs)** — each consumes indicator *deltas* and emits:
    - `strategy_signal_history` (append)
    - `strategy_signal_latest` (upsert)
4) **Allocator / TradePicker**
    - Applies budgets/limits; enforces **max 3 open symbols per account**;
    - SELLs allowed only if symbol currently open; writes:
    - `accepted_trade_signal_history` (append)
5) **TradingEngine**
    - Consumes accepted signals; processes Exchange **ExecReports** (fills);
    - Maintains **FIFO lots + rollup** in state; emits:
        - `trade_match_history` (append)
        - (prod) `open_positions_lots` (upsert/delete)
        - (prod) `open_positions_rollup` (upsert/delete)
        - (prod) `realized_pnl_latest` (upsert)
6) **Monitoring Job** (Unrealized PnL & Ops)
    - **Option A (metrics‑only):** expose Prometheus gauges (preferred live)
    - **Option B (table):** `unrealized_pnl_latest` (upsert) (optional)

**Backtest mode**: TradingEngine runs **matches‑only** (writes just `trade_match_history`); Monitoring can be off.

---

## 2) Core jobs & responsibilities
### Indicators Job
- Computes features per `(symbol, timeframe, barTs)`.
- Writes `indicator_history` (append) and `indicator_latest` (upsert).
- Guarantees `final=true` within **+5s** of bar close.

### Strategies (one job per strategy)
- React to **indicator deltas** only (filter `final=true` unless doing intrabar).
- Multi‑TF alignment uses an **anchor timeframe** (usually M1).
- Emit both history & latest; tag `strategyVersion`, `paramsHash`, optional `runId`.

### TradePicker (Open‑Position Gate)
- Keyed by **account** (or account+strategy if desired).
- Maintains **open symbols set** from `open_positions_rollup` deltas.
- Rules:
    - **BUY:** allow if symbol is already open or `openCount + pendingCount < 3`; otherwise reject `OPEN_LIMIT_EXCEEDED`.
    - **SELL:** require symbol currently open (strict: `netQty>0` if configured); otherwise reject `NO_OPEN_POSITION`.
    - Uses **reservations** to avoid races (pending open until rollup arrives).

### TradingEngine
- Stateful **PositionUpdater**:
    - FIFO lot management; rollup snapshot; cumulative **realized PnL**.
    - Emits **TradeMatch** rows for every matched segment.
    - Dedupe upstream **(orderId, fillId)** with MapState TTL (default **7d**).
- Side‑outputs (changelog):
    - **Lots** → `open_positions_lots` (UPSERT/DELETE)
    - **Rollup** → `open_positions_rollup` (UPSERT/DELETE; deletes when flat)
    - **Realized PnL latest** → `realized_pnl_latest` (UPSERT)
    - **TradeMatch** → `trade_match_history` (append)

### Monitoring Job (Unrealized)
- Symbol‑keyed join of **rollup deltas** + **1m price bars**.
- Compute `(price − avgPrice) × netQty`.
- **Default prod**: expose **Prometheus metrics** (totals per account/strategy, plus Top‑K symbols). Optional Fluss latest table.

### Paper (shadow) path
- **PaperBroker** simulates fills (e.g., next‑bar close ± slippage) for a **paper account namespace** (`paper:acct`), no cap on opens.
- Feeds the same TradingEngine → `paper_trade_match_history` for unbiased live scoring.

---

## 3) Fluss tables (contracts)
**Append‑only (history):**
- `price_1m_history(symbol, ts, price, …)`
- `indicator_history(symbol, timeframe, barTs, final, features…, calcTs, indicatorVersion)`
- `strategy_signal_history(strategyId, symbol, timeframe, signalTs, action, sizeHint, confidence, reason, strategyVersion, paramsHash, runId)`
- `accepted_trade_signal_history(accountId, strategyId, symbol, action, qty, ts, reason)`
- `exec_report_history(accountId, orderId, fillId, symbol, side, fillQty, fillPrice, ts, fees?, venue?)`
- `trade_match_history(matchId, accountId, strategyId, symbol, buyOrderId, sellOrderId, matchedQty, buyPrice, sellPrice, realizedPnl, matchTimestamp, …)`
- (Optional) `risk_alerts_history(accountId, code, message, ts)`
- (Paper) `paper_trade_match_history(… same as above …, runTag='PAPER_LIVE')`

**Latest / upsert (primary key; NOT ENFORCED):**
- `indicator_latest(symbol, timeframe) PK → (barTs, final, features…, calcTs, indicatorVersion)`
- `strategy_signal_latest(strategyId, symbol) PK → (timeframe, action, sizeHint, confidence, lastUpdated)`
- `open_positions_lots(accountId, strategyId, symbol, lotId) PK → (side, qtyOpen, qtyRem, avgPrice, tsOpen, tsUpdated)`
- `open_positions_rollup(accountId, strategyId, symbol) PK → (side, netQty, avgPrice, lastUpdated)`
- `realized_pnl_latest(accountId, strategyId, symbol) PK → (realizedPnl, ts)`
- **Optional:** `unrealized_pnl_latest(accountId, strategyId, symbol) PK → (unrealizedPnl, currentPrice, avgPrice, netQty, ts)`

**Notes:**
- Table bridge maps side‑output ops to **RowKind.UPDATE_AFTER/DELETE**; PKs are **NOT ENFORCED**.
- For unrealized, prefer **Prometheus metrics**; keep a table only if other jobs need SQL joins.

---

## 4) Keys, timing, correctness
- **Primary key for state/engine:** `(accountId, strategyId, symbol)`.
- **Event time** with bounded out‑of‑orderness:
    - Engine: **5s**; Monitoring: **2–5s**.
- **Dedupe:** per key, `MapState<String,Boolean>` on `(orderId,fillId)` with TTL **7 days**.
- **Deletes:** emit DELETE on lot close and when rollup netQty → 0; sinks must propagate real deletes.

---

## 5) Modes & toggles
- `engine.outputs = matches-only | full`
  • **matches-only** (backtest): write `trade_match_history` only.
  • **full** (prod): write lots, rollup, realized latest + matches.
- `monitoring.export = metrics | fluss | both` (default **metrics**)
- `tradepicker.maxOpenPositions = 3`
  `tradepicker.sellRequiresLong = true`
  `tradepicker.reserveOnOpen = true`
  `tradepicker.reservationTtlMs = 300000`
- `dedupe.ttlDays = 7`
  `wm.engine.latenessSec = 5`
  `wm.monitor.latenessSec = 2`
- `prices.timeframe = 1m`
  `fluss.freshness.price = 5–15s` (commit cadence)

---

## 6) Metrics & alerting (Prometheus)
**Gauges (per account/strategy):**
- `te_unrealized_pnl_total{account,strategy}`
- `te_realized_pnl_total{account,strategy}` (from engine state or cumulated matches)
- `te_net_exposure{account,strategy}`
- `te_open_positions_count{account,strategy}`

**Counters:**
- `tp_signals_accepted_total{account,reason="OK"}`
- `tp_signals_rejected_total{code}` (e.g., `OPEN_LIMIT_EXCEEDED`, `NO_OPEN_POSITION`)
- `engine_matches_emitted_total`

**Freshness / staleness:**
- `te_rollup_staleness_seconds{symbol}`; `prices_bar_delay_seconds{symbol}`

**Alerts:**
- Unrealized drawdown per account, stale data (>120s), frequent rejects, checkpoint duration spikes.

---

## 7) Backtests & analytics
- Backtests run **bounded** with `engine.outputs=matches-only`.
- Equity & stats from `trade_match_history` via the provided Python script (`equity_metrics.py`); default daily bucketing in **Europe/Oslo**.
- If not flat at end, optionally add a single unrealized snapshot from final prices.

---

## 8) Performance & scale notes
- With 1‑minute bars and ~20 strategies, volumes are modest.
- Partition by key; set operator parallelism ≥ active keys; use RocksDB + incremental checkpoints (10–15s).
- Symbol‑keyed Monitoring recomputes on **bar close** only; coalesce further if needed.

---

## 9) Weighting roadmap (future)
- Maintain **paper** scoreboard from `paper_trade_match_history`.
- Compute rolling, **shrunk** Sharpe / t‑stat per strategy (net of fees).
- Blend weights: `w = 50% equal + 50% score‑based`, with caps, floors, and change clamps.
- Publish to `strategy_perf_latest` / `strategy_weights_latest`; allocator scales `sizeHint × weight` while respecting TradePicker limits.

---

## 10) Runbooks
**First backtest:**
1) Set `engine.outputs=matches-only`; choose `runId`.
2) Bounded reads for signals/fills; ensure dedupe ON.
3) Produce `trade_match_history`; run `equity_metrics.py` for P&L/Sharpe/DD.

**Go‑live:**
1) Flip to `engine.outputs=full`; enable TradePicker and Monitoring (metrics).
2) Verify deletes flow to Fluss sinks; watch Prom freshness gauges.
3) Enable paper path → start `strategy_perf_latest` snapshots.

**Recovery/rescale:**
- Externalized checkpoints; increase parallelism to match active keys; Kafka optional (Fluss‑only is fine with 1m bars).

---

## 11) Glossary
- **Lot**: FIFO inventory unit; source of truth for open exposure.
- **Rollup**: per `(account,strategy,symbol)` netQty + avgPrice (non‑zero only).
- **Latest**: upsert table (PK) representing current state; deletes remove rows when flat.
- **Delta join**: react‑on‑change lookup/join between streams/tables (no full scans).
- **Paper**: shadow account with simulated fills for unbiased live scoring.

_End of snapshot._

