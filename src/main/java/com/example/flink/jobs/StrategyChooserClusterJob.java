package com.example.flink.jobs;

import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.sink.serializer.RowDataSerializationSchema;
import com.example.flink.domain.StrategySignal;
import com.example.flink.domain.AccountPolicy;
import com.example.flink.domain.Position;
import com.example.flink.domain.TradeSignal;
import com.example.flink.strategyengine.StrategyChooserJob;
import com.example.flink.tradeengine.fluss.Adapters.FlussRowMappers;
import com.example.flink.tradeengine.fluss.Adapters.PojoToFlussSink;
import com.example.flink.helpers.SerializableFunction;
import com.example.flink.tradeengine.fluss.Adapters.PositionLatestDeserializationSchema;
import com.example.flink.tradeengine.fluss.Adapters.StrategySignalDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import java.time.Duration;

/**
 * Cluster job that runs StrategyChooserJob using Fluss sources and sinks.
 * Sources:
 * - Strategy signals (e.g. fluss.strategy_signals) – wire your table
 * - Open positions latest (fluss.open_positions_latest)
 * - Account policies – here derived from signals; replace with real source as
 * needed
 * Sink:
 * - Accepted trade signals to fluss.trade_signal_history
 */
public class StrategyChooserClusterJob {
    public static void main(String[] args) throws Exception {
        String flussBootstrap = System.getProperty("fluss.bootstrap", "coordinator-server:9123");
        String flussDatabase = System.getProperty("fluss.database", "fluss");

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Strategy signals from Fluss (replace table name with your strategy output)
        var strategySignalSource = FlussSource.<StrategySignal>builder()
                .setBootstrapServers(flussBootstrap)
                .setDatabase(flussDatabase)
                .setTable("strategy_signals")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializationSchema(new StrategySignalDeserializationSchema())
                .build();
        DataStream<StrategySignal> strategySignals = env.fromSource(
                strategySignalSource,
                WatermarkStrategy.<StrategySignal>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((s, ts) -> s.timestamp),
                "Fluss Strategy Signals Source");

        // Positions latest from Fluss
        var positionSource = FlussSource.<Position>builder()
                .setBootstrapServers(flussBootstrap)
                .setDatabase(flussDatabase)
                .setTable("open_positions_latest")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializationSchema(new PositionLatestDeserializationSchema())
                .build();
        DataStream<Position> positions = env.fromSource(
                positionSource,
                WatermarkStrategy.<Position>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((p, ts) -> p.lastUpdated),
                "Fluss Positions Latest Source");

        // Policies: derive simple policy from signals (replace with real policy source)
        DataStream<AccountPolicy> policies = strategySignals
                .map(sig -> new AccountPolicy(sig.runId, 3, "ACTIVE", 100_000.0, sig.timestamp))
                .returns(AccountPolicy.class);

        // Sink for accepted trade signals
        var tradeSignalHistorySink = FlussSink.<RowData>builder()
                .setBootstrapServers(flussBootstrap)
                .setDatabase(flussDatabase)
                .setTable("trade_signal_history")
                .setSerializationSchema(new RowDataSerializationSchema(true, false))
                .build();

        var chooser = new StrategyChooserJob(
                strategySignals,
                policies,
                positions,
                new PojoToFlussSink<>(tradeSignalHistorySink, (SerializableFunction<TradeSignal, RowData>) FlussRowMappers::tradeSignalToRow));
        chooser.run();

        env.execute("Strategy Chooser Cluster Job");
    }
}
