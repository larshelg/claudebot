package com.example.flink.tradingengine2;

import com.example.flink.domain.TradeSignal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe append-only test sink for TradeSignal.
 * Safe for parallel test runs (multiple sink writers).
 */
public class TradeSignalCollectSink implements Sink<TradeSignal> {

    private static final ConcurrentLinkedQueue<TradeSignal> QUEUE = new ConcurrentLinkedQueue<>();
    private static final AtomicInteger COUNT = new AtomicInteger(0);

    /** Remove all collected elements. */
    public static void clear() {
        QUEUE.clear();
        COUNT.set(0);
    }

    /** Snapshot all collected signals (non-destructive). */
    public static List<TradeSignal> getAll() {
        return new ArrayList<>(QUEUE); // safe snapshot copy
    }

    /** O(1) size (approximate but consistent due to atomic updates). */
    public static int size() {
        return COUNT.get();
    }

    /** Atomically drain all collected signals (returns and clears). */
    public static List<TradeSignal> drain() {
        List<TradeSignal> out = new ArrayList<>(COUNT.get());
        TradeSignal t;
        while ((t = QUEUE.poll()) != null) {
            out.add(t);
            COUNT.decrementAndGet();
        }
        return out;
    }

    @Override
    public SinkWriter<TradeSignal> createWriter(InitContext context) {
        return new SinkWriter<>() {
            @Override
            public void write(TradeSignal element, Context ctx) {
                QUEUE.add(element);
                COUNT.incrementAndGet();
            }
            @Override public void flush(boolean endOfInput) {}
            @Override public void close() {}
        };
    }
}
