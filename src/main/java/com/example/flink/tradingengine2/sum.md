What we built

A TradingEngine pipeline that ingests execution reports (fills) and drives a stateful position engine in Flink, persisting to Fluss.

A PositionUpdater that is the core of the engine:

Maintains FIFO lots (per (accountId, strategyId, symbol)).

Maintains a rollup snapshot (netQty, side, weighted avg entry).

Computes cumulative realized PnL.

Emits trade-match audit events for every matched segment (buy leg vs sell leg).

Produces true changelog streams (UPSERT/DELETE) for lots and rollup.

Data model & semantics

Lots = source of truth for open exposure; each lot tracks side, qtyOpen, qtyRem, avgPrice, provenance (orderId#fillId).

Rollup = fast view per key with netQty, side (LONG/SHORT/flat), and weighted avgPrice over remaining lots.

Delete semantics:

When a lot reaches qtyRem=0 → emit DELETE for that lot.

When netQty=0 → emit DELETE for the rollup row.

TradeMatch:

One row per FIFO match segment, including both legs (entry & exit), matchedQty, and realizedPnl.

Realized PnL (latest):

Cumulative per (accountId, strategyId, symbol); updates on every close/cover.

Keys, timing, correctness

KeyBy: (accountId, strategyId, symbol) throughout.

Event time with bounded out-of-orderness watermarks (5s in the engine).

Dedupe: TTL’d per-key map of seen (orderId, fillId) to drop duplicate fills (protects lots, matches, and PnL).

Outputs & persistence

Side outputs:

LOTS_OUT → changelog for open_positions_lots (UPSERT/DELETE).

ROLLUP_OUT → changelog for open_positions_rollup (UPSERT/DELETE).

PNL_OUT → cumulative realized_pnl_latest (UPSERT).

MATCH_OUT → append-only trade_match_history.

Fluss DDL pack: history vs latest tables for orders, fills, lots, rollup, realized/unrealized PnL, portfolio, risk, and trade matches.

Table API bridge: maps side-outputs to Fluss tables using RowKind (INSERT/UPSERT/DELETE).

Testing we added (all green)

Test sinks for lots & rollup with:

A snapshot map (latest table view).

An op log to assert that DELETEs actually occurred.

Integration tests calling TradingEngine.build(...):

Partial close: validates remaining lot (qtyRem=2 @105) and rollup (netQty=2, avg=105).

Full cycle (build long → partial close → flip short → cover): final state flat + all expected DELETEs.

Realized PnL: +55 on partial close; +50 on full cycle.

TradeMatch history: 4 matches emitted with correct legs, quantities, and PnL breakdown.

Engineering hygiene

Converted PositionLot and PositionRollup to public Flink POJOs with no-arg ctors (fast, stable serialization); moved to a domain package.

Noted state compatibility if restoring from old savepoints (new class names → migrate or start fresh).

Clear path to exactly-once with checkpoints + transactional Fluss sink (and we still keep dedupe as a safety net).

What’s immediately next (if/when you want)

Add unrealized PnL (rollup × mid/last price) + exposure metrics.

Wire risk pre-checks (position count, symbol exposure, daily loss) before order submission.

Add metrics (fills processed, lots opened/closed, match latency) and alerts.

Optional: idempotent match IDs (deterministic matchId) if you want overwrite semantics downstream.

That’s the package: a robust, test-proven position engine with clean changelog outputs, audit-ready matches, and cumulative PnL—pluggable into Fluss for storage and downstream analytics.
