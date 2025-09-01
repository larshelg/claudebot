package com.example.flink.tradingengine2;

import com.example.flink.domain.TradeSignal;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Map;

/**
 * TradePicker (Account Router)
 *
 * Inputs:
 *  - StrategySignal (no accountId)
 *  - RollupChange (who holds what)
 *  - AccountPolicy (capacity + status)
 *
 * Output:
 *  - TradeSignal (with chosen accountId)
 *
 * Selection:
 *  - Consider only ACTIVE accounts with openCount < maxOpenSymbols.
 *  - Prefer accounts that DO NOT currently hold the symbol.
 *  - Tie-breaker: least openCount; then lexicographic accountId.
 *  - If no account has capacity => drop the signal.
 */
public final class TradePicker {

    private TradePicker() {}

    // --- Broadcast state descriptors (small control data) ---
    // Map: accountId -> meta (status, maxOpen, openCount)
    public static final MapStateDescriptor<String, AccountMeta> META_DESC =
            new MapStateDescriptor<>("accountMeta",
                    org.apache.flink.api.common.typeinfo.Types.STRING,
                    org.apache.flink.api.common.typeinfo.Types.POJO(AccountMeta.class));

    // Map: "accountId|symbol" -> Boolean(open)
    public static final MapStateDescriptor<String, Boolean> HELD_DESC =
            new MapStateDescriptor<>("heldSymbols",
                    org.apache.flink.api.common.typeinfo.Types.STRING,
                    org.apache.flink.api.common.typeinfo.Types.BOOLEAN);

    /** Wire the job. */
    public static DataStream<TradeSignal> build(StreamExecutionEnvironment env,
                                                DataStream<StrategySignal> strategySignals,
                                                DataStream<RollupChange> rollupDeltas,
                                                DataStream<AccountPolicy> policies,
                                                double defaultQty,
                                                boolean allowScaleInIfAlreadyHolding) {

        // Turn the two control streams into a single tagged control stream
        DataStream<Control> control =
                policies.map(Control::fromPolicy).returns(Control.class)
                        .union(
                                rollupDeltas.map(Control::fromPosition).returns(Control.class)
                        );

        // Key by symbol (so all decisions for a symbol are consistent),
        // and broadcast the control stream (policies + open/close deltas).
        BroadcastConnectedStream<StrategySignal, Control> connected =
                strategySignals.keyBy((KeySelector<StrategySignal, String>) s -> s.symbol)
                        .connect(control.broadcast(META_DESC, HELD_DESC));

        return connected
                .process(new RouterFn(defaultQty, allowScaleInIfAlreadyHolding))
                .name("TradePickerRouter");
    }

    // === Router ===
    static final class RouterFn extends KeyedBroadcastProcessFunction<String, StrategySignal, Control, TradeSignal> {
        private static final double EPS = 1e-12;

        private final double defaultQty;
        private final boolean allowScaleIn;

        RouterFn(double defaultQty, boolean allowScaleIn) {
            this.defaultQty = defaultQty;
            this.allowScaleIn = allowScaleIn;
        }

        @Override
        public void open(Configuration parameters) { /* no keyed state needed */ }

        // Handle strategy signals: choose an account and emit TradeSignal
        @Override
        public void processElement(StrategySignal s,
                                   ReadOnlyContext ctx,
                                   Collector<TradeSignal> out) throws Exception {
            ReadOnlyBroadcastState<String, AccountMeta> meta =
                    ctx.getBroadcastState(META_DESC);
            ReadOnlyBroadcastState<String, Boolean> held =
                    ctx.getBroadcastState(HELD_DESC);

            String chosen = null;
            int chosenOpen = Integer.MAX_VALUE;

            // Pass 1: prefer accounts that do NOT already hold the symbol
            Iterable<Map.Entry<String, AccountMeta>> accounts = meta.immutableEntries();
            if (accounts == null) return; // no policies yet

            for (Map.Entry<String, AccountMeta> e : accounts) {
                String acct = e.getKey();
                AccountMeta m = e.getValue();
                if (m == null) continue;

                if (!"ACTIVE".equalsIgnoreCase(m.status)) continue;
                if (m.openCount >= m.maxOpenSymbols) continue;

                boolean alreadyHolding = Boolean.TRUE.equals(held.get(acct + "|" + s.symbol));
                if (!alreadyHolding) {
                    if (m.openCount < chosenOpen || (m.openCount == chosenOpen && (chosen == null || acct.compareTo(chosen) < 0))) {
                        chosen = acct;
                        chosenOpen = m.openCount;
                    }
                }
            }

            // Pass 2: allow scale-in if configured and none picked in pass 1
            if (chosen == null && allowScaleIn) {
                for (Map.Entry<String, AccountMeta> e : meta.immutableEntries()) {
                    String acct = e.getKey();
                    AccountMeta m = e.getValue();
                    if (m == null) continue;

                    if (!"ACTIVE".equalsIgnoreCase(m.status)) continue;
                    if (m.openCount >= m.maxOpenSymbols) continue;

                    boolean alreadyHolding = Boolean.TRUE.equals(held.get(acct + "|" + s.symbol));
                    if (alreadyHolding) {
                        if (m.openCount < chosenOpen || (m.openCount == chosenOpen && (chosen == null || acct.compareTo(chosen) < 0))) {
                            chosen = acct;
                            chosenOpen = m.openCount;
                        }
                    }
                }
            }

            if (chosen == null) return; // no capacity anywhere

            TradeSignal t = new TradeSignal();
            t.signalId   = s.runId + "-" + s.timestamp;
            t.accountId  = chosen;
            t.strategyId = s.runId;
            t.symbol     = s.symbol;
            t.action     = s.signal;   // "BUY"/"SELL"
            t.qty        = defaultQty; // or your sizing logic
            t.ts         = s.timestamp;

            out.collect(t);
        }

        // Handle broadcast controls: update account meta and held symbols
        @Override
        public void processBroadcastElement(Control c,
                                            Context ctx,
                                            Collector<TradeSignal> out) throws Exception {
            BroadcastState<String, AccountMeta> meta = ctx.getBroadcastState(META_DESC);
            BroadcastState<String, Boolean> held = ctx.getBroadcastState(HELD_DESC);

            if (c.policy != null) {
                // Upsert policy → ensure meta row exists then update status/maxOpen
                AccountPolicy p = c.policy;
                AccountMeta m = meta.get(p.accountId);
                if (m == null) m = new AccountMeta(p.accountId, p.maxOpenSymbols, p.status, 0);
                m.maxOpenSymbols = p.maxOpenSymbols;
                m.status = p.status;
                meta.put(p.accountId, m);
                return;
            }

            if (c.positionDelta != null) {
                RollupChange r = c.positionDelta;
                String k = key(r.accountId, r.symbol);
                boolean isOpen = Math.abs(r.netQty) > EPS && r.op == Op.UPSERT;

                AccountMeta m = meta.get(r.accountId);
                if (m == null) {
                    // If we receive a position delta before policy, create a minimal ACTIVE row with huge capacity
                    m = new AccountMeta(r.accountId, Integer.MAX_VALUE / 2, "ACTIVE", 0);
                }

                Boolean prev = held.get(k);
                if (isOpen) {
                    if (prev == null || !prev) {
                        // transition: CLOSED -> OPEN
                        m.openCount += 1;
                        held.put(k, Boolean.TRUE);
                    } else {
                        // OPEN -> OPEN (qty change) : no count change
                        held.put(k, Boolean.TRUE);
                    }
                } else { // DELETE or net=0
                    if (prev != null && prev) {
                        // transition: OPEN -> CLOSED
                        m.openCount = Math.max(0, m.openCount - 1);
                        held.remove(k);
                    } // else CLOSED -> CLOSED: no-op
                }
                meta.put(r.accountId, m);
            }
        }

        private static String key(String accountId, String symbol) {
            return accountId + "|" + symbol;
        }
    }

    // === Data classes ===

    /** Small meta kept in broadcast state (per account). */
    public static class AccountMeta implements Serializable {
        public String accountId;
        public int maxOpenSymbols;
        public String status; // ACTIVE/BLOCKED
        public int openCount;

        public AccountMeta() {}
        public AccountMeta(String accountId, int maxOpenSymbols, String status, int openCount) {
            this.accountId = accountId;
            this.maxOpenSymbols = maxOpenSymbols;
            this.status = status;
            this.openCount = openCount;
        }
    }

    public static class StrategySignal implements Serializable {
        public String runId;
        public String symbol;
        public long timestamp;
        public double close;
        public double sma5;
        public double sma21;
        public String signal; // "BUY" / "SELL"
        public double signalStrength;
        public StrategySignal() {}
    }

    public enum Op { UPSERT, DELETE }

    /** Rollup changelog event (from open_positions_rollup). */
    public static class RollupChange implements Serializable {
        public Op op;
        public String accountId, strategyId, symbol;
        public double netQty, avgPrice;
        public long ts;
        public RollupChange() {}
        public static RollupChange upsert(String a, String s, String sym, double netQty, double avgPrice, long ts) {
            RollupChange r = new RollupChange();
            r.op=Op.UPSERT; r.accountId=a; r.strategyId=s; r.symbol=sym; r.netQty=netQty; r.avgPrice=avgPrice; r.ts=ts; return r;
        }
        public static RollupChange delete(String a, String s, String sym, long ts) {
            RollupChange r = new RollupChange();
            r.op=Op.DELETE; r.accountId=a; r.strategyId=s; r.symbol=sym; r.ts=ts; return r;
        }
    }

    /** Account policy events. */
    public static class AccountPolicy implements Serializable {
        public String accountId;
        public int maxOpenSymbols;
        public String status; // ACTIVE / BLOCKED
        public double initialCapital;
        public long ts;
        public AccountPolicy() {}
        public AccountPolicy(String accountId, int maxOpenSymbols, String status, long ts) {
            this(accountId, maxOpenSymbols, status, 100_000.0, ts);
        }
        public AccountPolicy(String accountId, int maxOpenSymbols, String status, double initialCapital, long ts) {
            this.accountId = accountId;
            this.maxOpenSymbols = maxOpenSymbols;
            this.status = status;
            this.initialCapital = initialCapital;
            this.ts = ts;
        }
    }

    /** Control wrapper for broadcast union. */
    public static class Control implements Serializable {
        public AccountPolicy policy;
        public RollupChange positionDelta;
        public Control() {}
        public static Control fromPolicy(AccountPolicy p) { Control c = new Control(); c.policy = p; return c; }
        public static Control fromPosition(RollupChange r) { Control c = new Control(); c.positionDelta = r; return c; }
    }

}
