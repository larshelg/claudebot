package com.example.flink.strategy;

import com.example.flink.domain.StrategySignal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/** Test sink: thread-safe append-only collector for StrategySignal. */
public class StrategySignalCollectSink implements Sink<StrategySignal> {

    private static final ConcurrentLinkedQueue<StrategySignal> Q = new ConcurrentLinkedQueue<>();
    private static final AtomicInteger COUNT = new AtomicInteger(0);

    public static void clear() { Q.clear(); COUNT.set(0); }

    /** Snapshot (non-destructive). */
    public static List<StrategySignal> getAll() { return new ArrayList<>(Q); }

    /** O(1) size. */
    public static int size() { return COUNT.get(); }

    /** Drain (returns and clears). */
    public static List<StrategySignal> drain() {
        List<StrategySignal> out = new ArrayList<>(COUNT.get());
        StrategySignal s;
        while ((s = Q.poll()) != null) {
            out.add(s);
            COUNT.decrementAndGet();
        }
        return out;
    }

    @Override
    public SinkWriter<StrategySignal> createWriter(InitContext context) {
        return new SinkWriter<>() {
            @Override public void write(StrategySignal element, Context ctx) {
                Q.add(element);
                COUNT.incrementAndGet();
            }
            @Override public void flush(boolean endOfInput) {}
            @Override public void close() {}
        };
    }
}
