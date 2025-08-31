package com.example.flink.tradingengine2;


import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Objects;

/**
 * TradePicker (Open-Position Gate)
 * Keyed by accountId. Maintains a Set<symbol> of open positions from rollup deltas.
 * On each incoming trade signal:
 *  - allow if symbol is already open, or openCount < maxOpenPositions,
 *  - otherwise reject with a clear reason.
 */
public final class TradePicker {

    private TradePicker() {}

    // Side outputs for visibility
    public static final OutputTag<RejectedSignal> REJECTED_OUT =
            new OutputTag<>("rejected-signal") {};

    /** Wire the job. */
    public static DataStream<AcceptedSignal> build(StreamExecutionEnvironment env,
                                                   DataStream<RollupChange> rollupDeltas,
                                                   DataStream<TradeSignal> signals,
                                                   int maxOpenPositions) {

        ConnectedStreams<RollupChange, TradeSignal> connected =
                rollupDeltas.keyBy((KeySelector<RollupChange, String>) r -> r.accountId)
                        .connect(
                                signals.keyBy((KeySelector<TradeSignal, String>) s -> s.accountId));

        DataStream<AcceptedSignal> accepted =
                connected.process(new GateFn(maxOpenPositions))
                        .name("OpenPositionGate");

        return accepted;
    }

    /** The gate operator. */
    // inside TradePicker

    static final class GateFn extends KeyedCoProcessFunction<String, RollupChange, TradeSignal, AcceptedSignal> {
        private static final double EPS = 1e-12;

        private final int maxOpen;
        private final boolean sellRequiresLong;   // if true: SELL only allowed when netQty > 0
        private final boolean reserveOnOpen;      // if true: create pending reservation to avoid race
        private final long   reservationTtlMs;    // TTL for pending reservations

        private transient MapState<String, PositionSnap> positions;   // symbol -> snapshot
        private transient ValueState<Integer> openCount;              // #symbols currently open
        private transient MapState<String, Long> pendingOpens;        // symbol -> expiryTs
        private transient ValueState<Integer> pendingCount;           // #pending

        GateFn(int maxOpen) {
            this(maxOpen, /*sellRequiresLong=*/true, /*reserveOnOpen=*/true, /*reservationTtlMs=*/5 * 60_000L);
        }
        GateFn(int maxOpen, boolean sellRequiresLong, boolean reserveOnOpen, long reservationTtlMs) {
            this.maxOpen = maxOpen; this.sellRequiresLong = sellRequiresLong;
            this.reserveOnOpen = reserveOnOpen; this.reservationTtlMs = reservationTtlMs;
        }

        @Override
        public void open(Configuration parameters) {
            positions = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("positions",
                            org.apache.flink.api.common.typeinfo.Types.STRING,
                            org.apache.flink.api.common.typeinfo.Types.POJO(PositionSnap.class)));

            openCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("openCount", org.apache.flink.api.common.typeinfo.Types.INT));

            pendingOpens = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("pendingOpens",
                            org.apache.flink.api.common.typeinfo.Types.STRING,
                            org.apache.flink.api.common.typeinfo.Types.LONG));

            pendingCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("pendingCount", org.apache.flink.api.common.typeinfo.Types.INT));

            // init
            tryInit(openCount, 0);
            tryInit(pendingCount, 0);
        }

        private static <T> void tryInit(ValueState<T> s, T v) {
            try { if (s.value() == null) s.update(v); } catch (Exception ignored) {}
        }

        // ==== Stream 1: rollup deltas keep positions & counters in sync ====
        @Override
        public void processElement1(RollupChange r, Context ctx, Collector<AcceptedSignal> out) throws Exception {
            final boolean isOpen = Math.abs(r.netQty) > EPS;

            PositionSnap prev = positions.get(r.symbol);
            boolean wasOpen = prev != null;

            if (!isOpen || r.op == Op.DELETE) {
                if (wasOpen) {
                    positions.remove(r.symbol);
                    openCount.update(openCount.value() - 1);
                }
                // clear any reservation
                if (pendingOpens.contains(r.symbol)) {
                    pendingOpens.remove(r.symbol);
                    pendingCount.update(Math.max(0, pendingCount.value() - 1));
                }
                return;
            }

            // UPSERT: now open
            positions.put(r.symbol, new PositionSnap(r.symbol, r.netQty, r.ts));
            if (!wasOpen) {
                openCount.update(openCount.value() + 1);
            }
            // resolve any reservation
            if (pendingOpens.contains(r.symbol)) {
                pendingOpens.remove(r.symbol);
                pendingCount.update(Math.max(0, pendingCount.value() - 1));
            }
        }

        // ==== Stream 2: signals get gated ====
        @Override
        public void processElement2(TradeSignal s, Context ctx, Collector<AcceptedSignal> out) throws Exception {
            cleanupExpiredReservations(ctx);

            final boolean isBuy = "BUY".equalsIgnoreCase(s.action);
            final boolean isSell = "SELL".equalsIgnoreCase(s.action);

            boolean alreadyOpen = positions.contains(s.symbol);
            boolean reserved    = pendingOpens.contains(s.symbol);
            int effOpen = openCount.value() + pendingCount.value();

            if (isBuy) {
                if (alreadyOpen || reserved || effOpen < maxOpen) {
                    // create a reservation if this opens a NEW symbol
                    if (!alreadyOpen && !reserved && reserveOnOpen) {
                        long exp = (s.ts > 0 ? s.ts : System.currentTimeMillis()) + reservationTtlMs;
                        pendingOpens.put(s.symbol, exp);
                        pendingCount.update(pendingCount.value() + 1);
                    }
                    out.collect(AcceptedSignal.from(s, "OK"));
                } else {
                    ctx.output(TradePicker.REJECTED_OUT,
                            RejectedSignal.from(s, "OPEN_LIMIT_EXCEEDED",
                                    "Account " + s.accountId + " has " + effOpen + " open (limit " + maxOpen + ")"));
                }
                return;
            }

            if (isSell) {
                PositionSnap ps = positions.get(s.symbol);
                if (ps == null) {
                    // Symbol is not open — likely closed by stop or another action
                    ctx.output(TradePicker.REJECTED_OUT,
                            RejectedSignal.from(s, "NO_OPEN_POSITION",
                                    "No open position for " + s.symbol + " on account " + s.accountId));
                    return;
                }
                if (sellRequiresLong && ps.netQty <= 0.0) {
                    // Strict mode: SELL allowed only when there's a long to exit
                    ctx.output(TradePicker.REJECTED_OUT,
                            RejectedSignal.from(s, "NO_LONG_TO_SELL",
                                    "Current net is " + ps.netQty + " (not long); SELL ignored"));
                    return;
                }
                out.collect(AcceptedSignal.from(s, "OK"));
                return;
            }

            // Other actions pass through by default (or handle explicitly)
            out.collect(AcceptedSignal.from(s, "OK"));
        }

        // Expire stale reservations (e.g., if rollup never arrived)
        private void cleanupExpiredReservations(Context ctx) throws Exception {
            long now = System.currentTimeMillis();
            int removed = 0;
            for (String sym : pendingOpens.keys()) {
                Long exp = pendingOpens.get(sym);
                if (exp != null && exp < now) {
                    pendingOpens.remove(sym);
                    removed++;
                }
            }
            if (removed > 0) pendingCount.update(Math.max(0, pendingCount.value() - removed));
        }
    }

    // ===== helper POJO kept small =====
    public static class PositionSnap implements Serializable {
        public PositionSnap() {}
        public String symbol;
        public double netQty;  // sign indicates side
        public long   ts;
        public PositionSnap(String symbol, double netQty, long ts) {
            this.symbol = symbol; this.netQty = netQty; this.ts = ts;
        }
    }

    // ======= POJOs =======

    public enum Op { UPSERT, DELETE }

    /** Rollup changelog event (from fluss.open_positions_rollup). */
    public static class RollupChange implements Serializable {
        public RollupChange() {}
        public Op op;
        public String accountId, strategyId, symbol;
        public double netQty, avgPrice;
        public long ts;
        public static RollupChange upsert(String a, String s, String sym, double netQty, double avgPrice, long ts) {
            RollupChange r = new RollupChange();
            r.op=Op.UPSERT; r.accountId=a; r.strategyId=s; r.symbol=sym; r.netQty=netQty; r.avgPrice=avgPrice; r.ts=ts; return r;
        }
        public static RollupChange delete(String a, String s, String sym, long ts) {
            RollupChange r = new RollupChange();
            r.op=Op.DELETE; r.accountId=a; r.strategyId=s; r.symbol=sym; r.ts=ts; return r;
        }
    }

    /** Incoming trade signal to gate. */
    public static class TradeSignal implements Serializable {
        public TradeSignal() {}
        public String signalId;
        public String accountId, strategyId, symbol;
        public String action;   // e.g., BUY/SELL/OPEN/CLOSE — gate is symbol-level
        public double qty;
        public long ts;
    }

    /** Accepted signal (passes through to engine/allocator). */
    public static class AcceptedSignal implements Serializable {
        public AcceptedSignal() {}
        public String signalId;
        public String accountId, strategyId, symbol;
        public String action;
        public double qty;
        public long ts;
        public String reason; // "OK" or note

        static AcceptedSignal from(TradeSignal s, String reason) {
            AcceptedSignal a = new AcceptedSignal();
            a.signalId=s.signalId; a.accountId=s.accountId; a.strategyId=s.strategyId;
            a.symbol=s.symbol; a.action=s.action; a.qty=s.qty; a.ts=s.ts; a.reason=reason; return a;
        }
    }

    /** Rejected signal (side output). */
    public static class RejectedSignal implements Serializable {
        public RejectedSignal() {}
        public String signalId;
        public String accountId, strategyId, symbol;
        public String action;
        public double qty;
        public long ts;
        public String code;    // e.g., OPEN_LIMIT_EXCEEDED
        public String message; // human-readable

        static RejectedSignal from(TradeSignal s, String code, String message) {
            RejectedSignal r = new RejectedSignal();
            r.signalId=s.signalId; r.accountId=s.accountId; r.strategyId=s.strategyId;
            r.symbol=s.symbol; r.action=s.action; r.qty=s.qty; r.ts=s.ts; r.code=code; r.message=message; return r;
        }
    }
}
