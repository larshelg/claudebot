flowchart LR


subgraph S[Strategies & Market Data]
MD[Market Data + Indicators]
STRAT[Strategy Engine]
MD --> STRAT
end


subgraph R[Pre-Trade Controls]
PRISK[Pre-Trade Risk Checks\n(max positions, exposure, capital)]
end


subgraph E[Execution]
EXEC[Execution Adapter]
EXCH[Exchange(s)]
end


subgraph M[Matching & State]
TM[Trade Match (FIFO/LIFO)\npartial fills, lot cascade]
PST[Flink Positions State\n(keyed by account,symbol)]
end


subgraph L[Fluss Tables]
LOTS[(open_positions_lots)]
ROLLUP[(open_positions_rollup)]
PNLR[(pnl_realized)]
PNLL[(pnl_latest)]
ORDERS[(orders, order_events)]
end


subgraph P[Portfolio & Risk]
PORT[Portfolio Updater]
RISK[Risk Engine\n(kill-switch, limits)]
end


STRAT -->|orders| PRISK
PRISK -->|approve| EXEC
PRISK -->|reject| STRAT


EXEC -->|order events| ORDERS
EXEC --> EXCH
EXCH -->|fills| TM


TM --> PST
TM --> PNLR


PST -->|upsert/delete| LOTS
PST -->|upsert| ROLLUP


PNLR --> PNLL


LOTS --> PORT
ROLLUP --> PORT
PORT --> RISK
RISK -->|limits update| PRISK


%% Optional feedback for strategy (position context, avg price, risk)
PORT --> STRAT
PNLL --> STRAT
