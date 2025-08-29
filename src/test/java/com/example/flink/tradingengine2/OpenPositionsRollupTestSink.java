package com.example.flink.tradingengine2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OpenPositionsRollupTestSink implements Sink<PositionUpdater.PositionRollupChange> {

    private static final Map<String, PositionUpdater.PositionRollup> latest = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedQueue<OpEvent> ops = new ConcurrentLinkedQueue<>();

    public static final class OpEvent {
        public final PositionUpdater.Op op;
        public final String accountId, strategyId, symbol;
        public OpEvent(PositionUpdater.Op op, String a, String s, String sym) {
            this.op = op; this.accountId=a; this.strategyId=s; this.symbol=sym;
        }
        @Override public String toString() { return op + " " + accountId + "|" + strategyId + "|" + symbol; }
    }

    private static String keyOf(PositionUpdater.RollupKey k) {
        return k.accountId + "|" + k.strategyId + "|" + k.symbol;
    }

    public static void clear() { latest.clear(); ops.clear(); }
    public static int size() { return latest.size(); }
    public static List<PositionUpdater.PositionRollup> getAll() { return new ArrayList<>(latest.values()); }
    public static List<OpEvent> getOpEvents() { return new ArrayList<>(ops); }

    public static boolean wasDeleted(String accountId, String strategyId, String symbol) {
        return ops.stream().anyMatch(e ->
                e.op == PositionUpdater.Op.DELETE &&
                        e.accountId.equals(accountId) &&
                        e.strategyId.equals(strategyId) &&
                        e.symbol.equals(symbol));
    }

    public static PositionUpdater.PositionRollup get(String accountId, String strategyId, String symbol) {
        return latest.get(accountId + "|" + strategyId + "|" + symbol);
    }

    @Override
    public SinkWriter<PositionUpdater.PositionRollupChange> createWriter(InitContext context) {
        return new SinkWriter<>() {
            @Override
            public void write(PositionUpdater.PositionRollupChange ch, Context ctx) {
                ops.add(new OpEvent(ch.op, ch.key.accountId, ch.key.strategyId, ch.key.symbol));
                if (ch.op == PositionUpdater.Op.UPSERT) {
                    latest.put(keyOf(ch.key), copy(ch.row));
                } else {
                    latest.remove(keyOf(ch.key));
                }
            }
            @Override public void flush(boolean endOfInput) {}
            @Override public void close() {}
        };
    }

    private static PositionUpdater.PositionRollup copy(PositionUpdater.PositionRollup r) {
        PositionUpdater.PositionRollup x = new PositionUpdater.PositionRollup();
        x.accountId = r.accountId; x.strategyId = r.strategyId; x.symbol = r.symbol;
        x.side = r.side; x.netQty = r.netQty; x.avgPrice = r.avgPrice; x.lastUpdated = r.lastUpdated;
        return x;
    }
}
