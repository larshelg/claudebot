sequenceDiagram
participant Strat as Strategy
participant Risk as Pre-Trade Risk
participant Exec as Execution Adapter
participant Exch as Exchange
participant TM as Trade Match (Flink)
participant PST as Positions State
participant Fluss as Fluss Tables (LOTS/ROLLUP/PNL)
participant Port as Portfolio Updater
participant RE as Risk Engine


Strat->>Risk: New Order (BUY/SELL, qty, limits)
Risk-->>Strat: Approved (or Reject)
Risk->>Exec: Forward Approved Order
Exec->>Exch: Place Order
Exch-->>Exec: Ack / Updates (order_events)
Exec->>Fluss: Write order_events
Exch-->>TM: Fill(s)
TM->>PST: Update lots (open/reduce/close)
TM->>Fluss: Upsert LOTS + ROLLUP (delete on close)
TM->>Fluss: Append pnl_realized
Fluss->>Port: Trigger portfolio recompute
Port->>RE: Update exposure, capital, risk metrics
RE->>Risk: Update active limits / kill-switch state
Fluss-->>Strat: (Optional) Notify avg_price, P&L context


KeyBy for TM and PST: (account_id, symbol) (optionally strategy_id if lots are per-strategy).

Idempotency: Use order_id + fill_id to dedupe. Persist last-processed offsets in state.

Backpressure: Decouple Exec and TM via Kafka topics (orders, order_events, fills) if needed.
