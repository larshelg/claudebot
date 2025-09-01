package com.example.flink.trend;

import com.example.flink.trend.TrendLineDetectorJob.TrendLine;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/** Thread-safe append-only collector for TrendLine (tests). */
public class TrendLineCollectSink implements Sink<TrendLine> {

    private static final ConcurrentLinkedQueue<TrendLine> Q = new ConcurrentLinkedQueue<>();
    private static final AtomicInteger COUNT = new AtomicInteger(0);

    public static void clear() { Q.clear(); COUNT.set(0); }
    public static int size() { return COUNT.get(); }
    public static List<TrendLine> drain() {
        List<TrendLine> out = new ArrayList<>(COUNT.get());
        TrendLine t;
        while ((t = Q.poll()) != null) {
            out.add(t); COUNT.decrementAndGet();
        }
        return out;
    }

    @Override
    public SinkWriter<TrendLine> createWriter(InitContext context) {
        return new SinkWriter<>() {
            @Override public void write(TrendLine element, Context context) { Q.add(element); COUNT.incrementAndGet(); }
            @Override public void flush(boolean endOfInput) {}
            @Override public void close() {}
        };
    }
}
