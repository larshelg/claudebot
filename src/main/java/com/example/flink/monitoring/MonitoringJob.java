package com.example.flink.monitoring;


import com.example.flink.domain.PriceTick;
import com.example.flink.domain.RollupChange;
import com.example.flink.domain.UnrealizedPnlChange;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class MonitoringJob {

    /**
     * Wire up Monitoring: Rollup changelog + Prices -> Unrealized PnL (UPSERT/DELETE).
     */
    public static void build(StreamExecutionEnvironment env,
                             DataStream<RollupChange> rollupChanges,
                             DataStream<PriceTick> priceTicks,
                             Sink<UnrealizedPnlChange> unrealizedSink) {

        DataStream<RollupChange> rollupsWm = rollupChanges.assignTimestampsAndWatermarks(
                WatermarkStrategy.<RollupChange>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((e, ts) -> e.ts)
        );

        DataStream<PriceTick> pricesWm = priceTicks.assignTimestampsAndWatermarks(
                WatermarkStrategy.<PriceTick>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((e, ts) -> e.ts)
        );

        pricesWm
                .keyBy((KeySelector<PriceTick, String>) p -> p.symbol)
                .connect(rollupsWm.keyBy((KeySelector<RollupChange, String>) r -> r.symbol))
                .process(new SymbolJoinUnrealizedFn())
                .name("UnrealizedPnL-BySymbol")
                .sinkTo(unrealizedSink)
                .name("UnrealizedPnL-Sink");
    }

    /** Optional: demo main with TODO sources/sinks. */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO: replace with real sources (e.g., Fluss rollup changelog source, Kafka prices)
        DataStream<RollupChange> rollups = env.fromElements();
        DataStream<PriceTick> prices = env.fromElements();

        // TODO: replace with Fluss sink to fluss.unrealized_pnl_latest
        Sink<UnrealizedPnlChange> sink = new Sinks.Blackhole<>();

        build(env, rollups, prices, sink);
        env.execute("MonitoringJob - Unrealized PnL");
    }

    /** Tiny blackhole sink placeholder. Replace with your Fluss sink. */
    static final class Sinks {
        static final class Blackhole<T> implements Sink<T> {
            @Override public SinkWriter<T> createWriter(InitContext context) { return new SinkWriter<>() {
                @Override public void write(T element, Context context) {}
                @Override public void flush(boolean endOfInput) {}
                @Override public void close() {}
            }; }
        }
    }
}
