package com.example.flink.tradingengine2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.example.flink.tradingengine2.PositionUpdater;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RealizedPnlLatestTestSink implements Sink<PositionUpdater.RealizedPnlChange> {
    private static final Map<String, PositionUpdater.RealizedPnlChange> latest = new ConcurrentHashMap<>();

    private static String keyOf(PositionUpdater.RealizedPnlChange r) {
        return r.accountId + "|" + r.strategyId + "|" + r.symbol;
    }

    public static void clear() { latest.clear(); }
    public static List<PositionUpdater.RealizedPnlChange> getAll() { return new ArrayList<>(latest.values()); }
    public static PositionUpdater.RealizedPnlChange get(String a, String s, String sym) { return latest.get(a+"|"+s+"|"+sym); }

    @Override
    public SinkWriter<PositionUpdater.RealizedPnlChange> createWriter(InitContext context) {
        return new SinkWriter<>() {
            @Override public void write(PositionUpdater.RealizedPnlChange v, Context ctx) { latest.put(keyOf(v), v); }
            @Override public void flush(boolean endOfInput) {}
            @Override public void close() {}
        };
    }
}
