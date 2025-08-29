package com.example.flink.tradingengine2;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OpenPositionsLotsTestSink implements Sink<PositionUpdater.PositionLotChange> {

    private static final Map<String, PositionUpdater.PositionLot> latest = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedQueue<OpEvent> ops = new ConcurrentLinkedQueue<>();

    public static final class OpEvent {
        public final PositionUpdater.Op op;
        public final String accountId, strategyId, symbol, lotId;
        public OpEvent(PositionUpdater.Op op, String a, String s, String sym, String l) {
            this.op = op; this.accountId=a; this.strategyId=s; this.symbol=sym; this.lotId=l;
        }
        @Override public String toString() { return op + " " + accountId + "|" + strategyId + "|" + symbol + "|" + lotId; }
    }

    private static String keyOf(PositionUpdater.LotKey k) {
        return k.accountId + "|" + k.strategyId + "|" + k.symbol + "|" + k.lotId;
    }

    public static void clear() { latest.clear(); ops.clear(); }
    public static int size() { return latest.size(); }
    public static List<PositionUpdater.PositionLot> getAll() { return new ArrayList<>(latest.values()); }
    public static List<OpEvent> getOpEvents() { return new ArrayList<>(ops); }

    public static boolean wasDeleted(String accountId, String strategyId, String symbol, String lotId) {
        return ops.stream().anyMatch(e ->
                e.op == PositionUpdater.Op.DELETE &&
                        e.accountId.equals(accountId) &&
                        e.strategyId.equals(strategyId) &&
                        e.symbol.equals(symbol) &&
                        e.lotId.equals(lotId));
    }

    public static PositionUpdater.PositionLot get(String accountId, String strategyId, String symbol, String lotId) {
        return latest.get(accountId + "|" + strategyId + "|" + symbol + "|" + lotId);
    }

    @Override
    public SinkWriter<PositionUpdater.PositionLotChange> createWriter(InitContext context) {
        return new SinkWriter<>() {
            @Override
            public void write(PositionUpdater.PositionLotChange ch, Context ctx) {
                ops.add(new OpEvent(ch.op, ch.key.accountId, ch.key.strategyId, ch.key.symbol, ch.key.lotId));
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

    private static PositionUpdater.PositionLot copy(PositionUpdater.PositionLot l) {
        PositionUpdater.PositionLot x = new PositionUpdater.PositionLot();
        x.accountId = l.accountId; x.strategyId = l.strategyId; x.symbol = l.symbol; x.lotId = l.lotId;
        x.side = l.side; x.qtyOpen = l.qtyOpen; x.qtyRem = l.qtyRem; x.avgPrice = l.avgPrice;
        x.tsOpen = l.tsOpen; x.tsUpdated = l.tsUpdated; return x;
    }
}
