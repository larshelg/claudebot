package com.example.flink.trend;

import com.example.flink.trend.TrendSummarizerFn.TrendSummary;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/** Test sink for TrendSummary (append-only, thread-safe). */
public class TrendSummaryCollectSink implements Sink<TrendSummary> {

    private static final ConcurrentLinkedQueue<TrendSummary> Q = new ConcurrentLinkedQueue<>();
    private static final AtomicInteger COUNT = new AtomicInteger(0);

    public static void clear() { Q.clear(); COUNT.set(0); }
    public static int size() { return COUNT.get(); }

    /** Drain and clear. */
    public static List<TrendSummary> drain() {
        var out = new ArrayList<TrendSummary>(COUNT.get());
        TrendSummary s;
        while ((s = Q.poll()) != null) {
            out.add(s); COUNT.decrementAndGet();
        }
        return out;
    }

    @Override
    public SinkWriter<TrendSummary> createWriter(InitContext context) {
        return new SinkWriter<>() {
            @Override public void write(TrendSummary element, Context ctx) {
                Q.add(element); COUNT.incrementAndGet();
            }
            @Override public void flush(boolean endOfInput) {}
            @Override public void close() {}
        };
    }
}
