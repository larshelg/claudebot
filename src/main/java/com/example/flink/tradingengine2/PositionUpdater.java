package com.example.flink.tradingengine2;

import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;

/**
 * PositionUpdater
 * Keyed by (accountId, strategyId, symbol).
 *
 * Responsibilities
 * - Maintain FIFO lots (LONG/SHORT) in keyed state.
 * - Maintain rollup snapshot (netQty, avgPrice, side).
 * - Compute & emit cumulative Realized PnL updates.
 * - Emit TradeMatch rows for each matched segment (audit).
 * - Emit true changelog events for lots/rollup (UPSERT/DELETE) via side outputs.
 *
 * Side outputs (defined in TradingEngine):
 *   LOTS_OUT   -> PositionLotChange (UPSERT/DELETE)
 *   ROLLUP_OUT -> PositionRollupChange (UPSERT/DELETE)
 *   PNL_OUT    -> RealizedPnlChange (UPSERT only, cumulative)
 *   MATCH_OUT  -> TradeMatch (append-only)
 */
public class PositionUpdater extends KeyedProcessFunction<PositionUpdater.PositionKey,
        PositionUpdater.ExecReport, PositionUpdater.VoidOut> {

    private static final double EPS = 1e-12;

    // --- Flink state ---
    private transient ListState<PositionLot> lotsState;
    private transient ValueState<PositionRollup> rollupState;
    private transient ValueState<Double> realizedPnlCumState;
    private transient MapState<String, Boolean> seenFills;

    @Override
    public void open(Configuration parameters) {
        lotsState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("lots", PositionLot.class));
        rollupState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("rollup", PositionRollup.class));
        realizedPnlCumState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("realizedPnlCum", Double.class));

        StateTtlConfig ttl = StateTtlConfig
                .newBuilder(org.apache.flink.api.common.time.Time.days(7))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000) // safe no-op on heap state
                .build();

        MapStateDescriptor<String, Boolean> desc =
                new MapStateDescriptor<>("seenFills", org.apache.flink.api.common.typeinfo.Types.STRING, org.apache.flink.api.common.typeinfo.Types.BOOLEAN);
        desc.enableTimeToLive(ttl);
        seenFills = getRuntimeContext().getMapState(desc);
    }

    private static String dedupeId(ExecReport f) {
        if (f.fillId != null && !f.fillId.isEmpty()) {
            return f.accountId + "|" + f.strategyId + "|" + f.symbol + "|" + f.orderId + "|" + f.fillId;
        }
        // Fallback: stable hash when fillId missing
        String raw = f.accountId + "|" + f.strategyId + "|" + f.symbol + "|" + f.orderId + "|" +
                f.side + "|" + f.fillQty + "|" + f.fillPrice + "|" + f.ts;
        return Integer.toHexString(raw.hashCode());
    }

    @Override
    public void processElement(ExecReport fill,
                               Context ctx,
                               Collector<VoidOut> out) throws Exception {

        String eid = dedupeId(fill);
        if (seenFills.contains(eid)) {
            return; // drop duplicate fill
        }
        seenFills.put(eid, Boolean.TRUE);


        // restore state
        Deque<PositionLot> lots = restoreLotsInFifoOrder();
        PositionRollup roll = rollupState.value();
        if (roll == null) roll = PositionRollup.empty(fill.accountId, fill.strategyId, fill.symbol);
        double realizedCum = Optional.ofNullable(realizedPnlCumState.value()).orElse(0.0);

        final boolean isBuy = "BUY".equalsIgnoreCase(fill.side);
        double remaining = fill.fillQty;
        final double price = fill.fillPrice;
        final long now = fill.ts;

        List<PositionLotChange> lotChanges = new ArrayList<>();
        List<TradeMatch> matches = new ArrayList<>();
        double realizedDelta = 0.0;

        if (isBuy) {
            // Close SHORT exposure first
            if (roll.netQty < -EPS) {
                ConsumeResult cr = consumeAgainstExistingLotsAndComputePnL(
                        lots, lotChanges, matches, fill, remaining, price, now, /*closingLong=*/false);
                realizedDelta += cr.realized;
                remaining -= cr.consumed;
            }
            // Open LONG with any remainder
            if (remaining > EPS) {
                PositionLot lot = PositionLot.openNew(fill, remaining, price, now, "LONG");
                lots.addLast(lot);
                lotChanges.add(PositionLotChange.upsert(lot));
                remaining = 0.0;
            }
        } else {
            // SELL: close LONG exposure first
            if (roll.netQty > EPS) {
                ConsumeResult cr = consumeAgainstExistingLotsAndComputePnL(
                        lots, lotChanges, matches, fill, remaining, price, now, /*closingLong=*/true);
                realizedDelta += cr.realized;
                remaining -= cr.consumed;
            }
            // Open SHORT with any remainder
            if (remaining > EPS) {
                PositionLot lot = PositionLot.openNew(fill, remaining, price, now, "SHORT");
                lots.addLast(lot);
                lotChanges.add(PositionLotChange.upsert(lot));
                remaining = 0.0;
            }
        }

        // recompute rollup from lots
        PositionRollup newRoll = PositionRollup.fromLots(lots, fill.accountId, fill.strategyId, fill.symbol, now);

        // emit rollup change
        if (Math.abs(newRoll.netQty) <= EPS) {
            ctx.output(TradingEngine.ROLLUP_OUT, PositionRollupChange.delete(newRoll.key(), now));
        } else {
            ctx.output(TradingEngine.ROLLUP_OUT, PositionRollupChange.upsert(newRoll));
        }

        // emit lot changes
        for (PositionLotChange ch : lotChanges) {
            ctx.output(TradingEngine.LOTS_OUT, ch);
        }

        // emit trade matches (append-only)
        for (TradeMatch m : matches) {
            ctx.output(TradingEngine.MATCH_OUT, m);
        }

        // emit realized PnL latest (cumulative)
        if (Math.abs(realizedDelta) > EPS) {
            realizedCum += realizedDelta;
            realizedPnlCumState.update(realizedCum);
            ctx.output(TradingEngine.PNL_OUT, RealizedPnlChange.upsert(
                    fill.accountId, fill.strategyId, fill.symbol, realizedCum, now));
        }

        // persist state
        snapshotLots(lots);
        rollupState.update(newRoll);
    }

    // --- FIFO consumption with PnL + TradeMatch emission ---
    private ConsumeResult consumeAgainstExistingLotsAndComputePnL(
            Deque<PositionLot> lots,
            List<PositionLotChange> outChanges,
            List<TradeMatch> outMatches,
            ExecReport exitFill,          // the closing fill (BUY when closing short, SELL when closing long)
            double qtyToClose,
            double exitPrice,
            long ts,
            boolean closingLong) {        // true when reducing LONG via SELL; false when reducing SHORT via BUY

        double realized = 0.0;
        double consumed = 0.0;

        while (qtyToClose > EPS && !lots.isEmpty()) {
            PositionLot head = lots.peekFirst();

            // Only consume from matching side
            boolean headMatches = closingLong
                    ? "LONG".equals(head.side)
                    : "SHORT".equals(head.side);
            if (!headMatches) break;

            double take = Math.min(qtyToClose, head.qtyRem);

            // PnL leg:
            // - Closing LONG with SELL: (exit - entry) * qty
            // - Closing SHORT with BUY: (entry - exit) * qty
            double legPnl = closingLong
                    ? (exitPrice - head.avgPrice) * take
                    : (head.avgPrice - exitPrice) * take;
            realized += legPnl;

            // TradeMatch record
            TradeMatch tm;
            if (closingLong) {
                // Entry BUY (lot) -> Exit SELL (fill)
                tm = TradeMatch.of(
                        head.accountId, head.strategyId, head.symbol,
                        head.sourceOrderId, head.sourceFillId, head.tsOpen, head.avgPrice,
                        exitFill.orderId, exitFill.fillId, exitFill.ts, exitPrice,
                        take, legPnl, ts);
            } else {
                // Entry SELL (lot) -> Exit BUY (fill)
                tm = TradeMatch.of(
                        head.accountId, head.strategyId, head.symbol,
                        exitFill.orderId, exitFill.fillId, exitFill.ts, exitPrice,
                        head.sourceOrderId, head.sourceFillId, head.tsOpen, head.avgPrice,
                        take, legPnl, ts);
            }
            outMatches.add(tm);

            // Update FIFO lot
            head.qtyRem -= take;
            head.tsUpdated = ts;
            if (head.qtyRem <= EPS) {
                lots.removeFirst();
                outChanges.add(PositionLotChange.delete(head.key(), ts));
            } else {
                outChanges.add(PositionLotChange.upsert(head));
            }

            qtyToClose -= take;
            consumed += take;
        }

        return new ConsumeResult(realized, consumed);
    }

    // --- state helpers ---
    private Deque<PositionLot> restoreLotsInFifoOrder() throws Exception {
        List<PositionLot> list = new ArrayList<>();
        for (PositionLot l : lotsState.get()) list.add(l);
        // deterministic FIFO: open time then lotId
        list.sort(Comparator
                .comparingLong((PositionLot l) -> l.tsOpen)
                .thenComparing(l -> l.lotId));
        return new ArrayDeque<>(list);
    }

    private void snapshotLots(Deque<PositionLot> lots) throws Exception {
        lotsState.clear();
        for (PositionLot l : lots) lotsState.add(l);
    }

    // ======== Small DTO for consumption result ========
    static final class ConsumeResult {
        final double realized;
        final double consumed;
        ConsumeResult(double realized, double consumed) {
            this.realized = realized; this.consumed = consumed;
        }
    }

    // ======== Types, Keys, Changes, POJOs ========

    /** Void downstream element; all outputs are via side outputs. */
    static final class VoidOut {}

    /** Composite key: (accountId, strategyId, symbol). */
    static final class PositionKey implements Serializable {
        final String accountId;
        final String strategyId;
        final String symbol;
        PositionKey(String accountId, String strategyId, String symbol) {
            this.accountId = accountId; this.strategyId = strategyId; this.symbol = symbol;
        }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PositionKey)) return false;
            PositionKey that = (PositionKey) o;
            return Objects.equals(accountId, that.accountId)
                    && Objects.equals(strategyId, that.strategyId)
                    && Objects.equals(symbol, that.symbol);
        }
        @Override public int hashCode() { return Objects.hash(accountId, strategyId, symbol); }
        @Override public String toString() { return accountId + "|" + strategyId + "|" + symbol; }
    }

    /** ExecReport (fill) from the exchange executor. */
    public static final class ExecReport implements Serializable {
        String accountId;
        String strategyId;
        String orderId;
        String fillId;
        String symbol;
        String side;     // BUY / SELL
        double fillQty;
        double fillPrice;
        long ts;         // event time (millis)

        public ExecReport() {}
        public ExecReport(String accountId, String strategyId, String orderId, String fillId,
                          String symbol, String side, double fillQty, double fillPrice, long ts) {
            this.accountId = accountId; this.strategyId = strategyId; this.orderId = orderId; this.fillId = fillId;
            this.symbol = symbol; this.side = side; this.fillQty = fillQty; this.fillPrice = fillPrice; this.ts = ts;
        }
    }

    /** One FIFO lot. */
    public static final class PositionLot implements Serializable {
        String accountId;
        String strategyId;
        String symbol;
        String lotId;
        String side;         // LONG / SHORT
        double qtyOpen;      // original opened qty
        double qtyRem;       // remaining qty
        double avgPrice;     // entry price
        long tsOpen;         // open timestamp
        long tsUpdated;      // last update timestamp

        // provenance for building TradeMatch buy/sell legs
        String sourceOrderId;  // opening order id
        String sourceFillId;   // opening fill id

        public PositionLot() {}
        static PositionLot openNew(ExecReport er, double qty, double price, long now, String side) {
            PositionLot l = new PositionLot();
            l.accountId = er.accountId;
            l.strategyId = er.strategyId;
            l.symbol = er.symbol;
            l.lotId = (er.orderId != null && er.fillId != null)
                    ? er.orderId + "#" + er.fillId
                    : UUID.randomUUID().toString();
            l.side = side;
            l.qtyOpen = qty;
            l.qtyRem = qty;
            l.avgPrice = price;
            l.tsOpen = now;
            l.tsUpdated = now;
            l.sourceOrderId = er.orderId;
            l.sourceFillId = er.fillId;
            return l;
        }

        LotKey key() { return new LotKey(accountId, strategyId, symbol, lotId); }
    }

    /** Rollup snapshot per account/strategy/symbol. */
    public static class PositionRollup implements Serializable {
        String accountId;
        String strategyId;
        String symbol;
        String side;       // LONG if netQty>0, SHORT if <0, null if flat
        double netQty;
        double avgPrice;   // weighted avg of remaining lots (positive)
        long lastUpdated;

        public PositionRollup() {}

        static PositionRollup empty(String accountId, String strategyId, String symbol) {
            PositionRollup r = new PositionRollup();
            r.accountId = accountId; r.strategyId = strategyId; r.symbol = symbol;
            r.side = null; r.netQty = 0.0; r.avgPrice = 0.0; r.lastUpdated = Instant.now().toEpochMilli();
            return r;
        }

        static PositionRollup fromLots(Deque<PositionLot> lots, String accountId, String strategyId, String symbol, long ts) {
            double qty = 0.0;
            double notional = 0.0;
            for (PositionLot l : lots) {
                double signed = "LONG".equals(l.side) ? +l.qtyRem : -l.qtyRem;
                qty += signed;
                notional += l.avgPrice * signed;
            }
            PositionRollup r = new PositionRollup();
            r.accountId = accountId; r.strategyId = strategyId; r.symbol = symbol;
            r.netQty = qty;
            r.side = (qty > EPS) ? "LONG" : (qty < -EPS) ? "SHORT" : null;
            r.avgPrice = Math.abs(qty) > EPS ? Math.abs(notional / qty) : 0.0; // positive avg price
            r.lastUpdated = ts;
            return r;
        }

        RollupKey key() { return new RollupKey(accountId, strategyId, symbol); }
    }

    // Trade match (append-only)
    static final class TradeMatch implements Serializable {
        String matchId;
        String accountId, strategyId, symbol;
        String buyOrderId, buyFillId;   long buyTs;   double buyPrice;
        String sellOrderId, sellFillId; long sellTs;  double sellPrice;
        double matchedQty;
        double realizedPnl;
        long matchTs;

        static TradeMatch of(String accountId, String strategyId, String symbol,
                             String buyOrderId, String buyFillId, long buyTs, double buyPrice,
                             String sellOrderId, String sellFillId, long sellTs, double sellPrice,
                             double qty, double realizedPnl, long matchTs) {
            TradeMatch m = new TradeMatch();
            m.matchId = UUID.randomUUID().toString();
            m.accountId = accountId; m.strategyId = strategyId; m.symbol = symbol;
            m.buyOrderId = buyOrderId; m.buyFillId = buyFillId; m.buyTs = buyTs; m.buyPrice = buyPrice;
            m.sellOrderId = sellOrderId; m.sellFillId = sellFillId; m.sellTs = sellTs; m.sellPrice = sellPrice;
            m.matchedQty = qty; m.realizedPnl = realizedPnl; m.matchTs = matchTs;
            return m;
        }
    }

    // ---- Changelog model ----
    enum Op { UPSERT, DELETE }

    static final class LotKey implements Serializable {
        final String accountId, strategyId, symbol, lotId;
        LotKey(String a, String s, String sym, String l) { this.accountId=a; this.strategyId=s; this.symbol=sym; this.lotId=l; }
    }

    static final class RollupKey implements Serializable {
        final String accountId, strategyId, symbol;
        RollupKey(String a, String s, String sym) { this.accountId=a; this.strategyId=s; this.symbol=sym; }
    }

    static final class PositionLotChange implements Serializable {
        final Op op;
        final LotKey key;
        final PositionLot row;  // present when UPSERT
        final long ts;

        private PositionLotChange(Op op, LotKey key, PositionLot row, long ts) {
            this.op = op; this.key = key; this.row = row; this.ts = ts;
        }
        static PositionLotChange upsert(PositionLot l) {
            return new PositionLotChange(Op.UPSERT, l.key(), l, l.tsUpdated);
        }
        static PositionLotChange delete(LotKey key, long ts) {
            return new PositionLotChange(Op.DELETE, key, null, ts);
        }
    }

    static final class PositionRollupChange implements Serializable {
        final Op op;
        final RollupKey key;
        final PositionRollup row; // present when UPSERT
        final long ts;

        private PositionRollupChange(Op op, RollupKey key, PositionRollup row, long ts) {
            this.op = op; this.key = key; this.row = row; this.ts = ts;
        }
        static PositionRollupChange upsert(PositionRollup r) {
            return new PositionRollupChange(Op.UPSERT, r.key(), r, r.lastUpdated);
        }
        static PositionRollupChange delete(RollupKey key, long ts) {
            return new PositionRollupChange(Op.DELETE, key, null, ts);
        }
    }

    // Cumulative realized PnL (latest)
    static final class RealizedPnlChange implements Serializable {
        final String accountId, strategyId, symbol;
        final double realizedPnl;
        final long ts;

        private RealizedPnlChange(String accountId, String strategyId, String symbol, double realizedPnl, long ts) {
            this.accountId = accountId; this.strategyId = strategyId; this.symbol = symbol;
            this.realizedPnl = realizedPnl; this.ts = ts;
        }
        static RealizedPnlChange upsert(String accountId, String strategyId, String symbol, double realizedPnl, long ts) {
            return new RealizedPnlChange(accountId, strategyId, symbol, realizedPnl, ts);
        }
    }
}
