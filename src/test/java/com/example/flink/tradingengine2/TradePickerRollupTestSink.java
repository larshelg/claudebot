package com.example.flink.tradingengine2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Test sink for TradePicker.RollupChange.
 * Records the latest PositionSnap-like row per (account|strategy|symbol) and an op log.
 */
public class TradePickerRollupTestSink implements Sink<TradePicker.RollupChange> {

    private static final Map<String, Row> latest = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedQueue<OpEvent> ops = new ConcurrentLinkedQueue<>();

    // Lightweight row copy (so tests don't rely on POJO identity)
    public static final class Row {
        public String accountId, strategyId, symbol;
        public double netQty, avgPrice;
        public long ts;

        public Row() {}
        public Row(String a, String s, String sym, double netQty, double avgPrice, long ts) {
            this.accountId = a; this.strategyId = s; this.symbol = sym;
            this.netQty = netQty; this.avgPrice = avgPrice; this.ts = ts;
        }

        @Override public String toString() {
            return "Row{" + accountId + "|" + strategyId + "|" + symbol +
                    ", netQty=" + netQty + ", avgPrice=" + avgPrice + ", ts=" + ts + "}";
        }
    }

    public static final class OpEvent {
        public final TradePicker.Op op;
        public final String accountId, strategyId, symbol;
        public OpEvent(TradePicker.Op op, String a, String s, String sym) {
            this.op = op; this.accountId = a; this.strategyId = s; this.symbol = sym;
        }
        @Override public String toString() { return op + " " + accountId + "|" + strategyId + "|" + symbol; }
    }

    private static String keyOf(String accountId, String strategyId, String symbol) {
        return accountId + "|" + strategyId + "|" + symbol;
    }

    // -------- Test helpers --------
    public static void clear() { latest.clear(); ops.clear(); }
    public static int size() { return latest.size(); }
    public static List<Row> getAll() { return new ArrayList<>(latest.values()); }
    public static List<OpEvent> getOpEvents() { return new ArrayList<>(ops); }

    public static boolean wasDeleted(String accountId, String strategyId, String symbol) {
        return ops.stream().anyMatch(e ->
                e.op == TradePicker.Op.DELETE &&
                        e.accountId.equals(accountId) &&
                        e.strategyId.equals(strategyId) &&
                        e.symbol.equals(symbol));
    }

    public static Row get(String accountId, String strategyId, String symbol) {
        return latest.get(keyOf(accountId, strategyId, symbol));
    }

    // -------- Sink impl --------
    @Override
    public SinkWriter<TradePicker.RollupChange> createWriter(InitContext context) {
        return new SinkWriter<>() {
            @Override
            public void write(TradePicker.RollupChange ch, Context ctx) {
                ops.add(new OpEvent(ch.op, ch.accountId, ch.strategyId, ch.symbol));
                String key = keyOf(ch.accountId, ch.strategyId, ch.symbol);
                if (ch.op == TradePicker.Op.UPSERT) {
                    latest.put(key, copy(ch));
                } else {
                    latest.remove(key);
                }
            }
            @Override public void flush(boolean endOfInput) {}
            @Override public void close() {}
        };
    }

    private static Row copy(TradePicker.RollupChange ch) {
        return new Row(ch.accountId, ch.strategyId, ch.symbol, ch.netQty, ch.avgPrice, ch.ts);
    }
}
