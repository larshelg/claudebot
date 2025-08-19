                 ┌──────────────────┐
                 │  Accounts Table  │   (static config in Fluss)
                 │ (limits, status) │
                 └───────┬──────────┘
                         │ (delta/broadcast join)
                         ▼
┌──────────────────┐   ┌───────────────────────┐
│  Trade Signals   │──▶│ Risk Pre-Check (SQL)  │───┐
│  (from Job A/B)  │   │ join with Accounts     │   │
└──────────────────┘   │ filter: exposure etc. │   │
                       └──────────┬────────────┘   │
                                  │                │
                                  ▼                │
                       ┌───────────────────┐       │
                       │  Accepted Orders  │──────▶│
                       │   (to Job D)      │       │
                       └───────────────────┘       │
                                                   │
                                                   │
┌──────────────────────┐                           │
│ Execution Reports    │ (from Job D: FILLS/ACKS)  │
└──────────────────────┘                           │
             │                                    │
             ▼                                    │
 ┌────────────────────┐                           │
 │ PositionUpdater    │  keyed by (account,symbol)│
 │ - net qty          │◀──────────────────────────┘
 │ - avg px           │
 │ - realized/unreal. │
 └─────────┬──────────┘
           │ emits updates
           ▼
 ┌────────────────────┐
 │ positions_state    │   (Fluss table: per account+symbol)
 └─────────┬──────────┘
           │
           ▼
 ┌────────────────────┐
 │ PortfolioUpdater   │ keyed by (account)
 │ - cash balance     │
 │ - equity           │
 │ - margin used      │
 │ - exposure         │
 └─────────┬──────────┘
           │ emits updates
           ▼
 ┌────────────────────┐
 │ portfolio_state    │   (Fluss table: per account)
 └─────────┬──────────┘
           │
           ▼
 ┌────────────────────┐
 │ Risk Engine        │
 │ - checks vs limits │
 │ - kill switch      │
 │ - risk alerts      │
 └─────────┬──────────┘
           │
           ▼
   ┌──────────────┐
   │ risk_alerts  │ (to dashboards / monitoring)
   │ kill_signals │ (to Job D – stops execution)
   └──────────────┘
