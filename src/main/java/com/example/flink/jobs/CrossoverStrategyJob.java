package com.example.flink.jobs;

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.sink.serializer.RowDataSerializationSchema;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.example.flink.GenericStrategyJob;
import com.example.flink.serializer.*;
import com.example.flink.strategy.SimpleCrossoverStrategy;
import com.example.flink.strategy.TradingStrategy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import java.time.Duration;
import java.util.List;
import java.util.Set;

public class CrossoverStrategyJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. Sources
        List<String> projectedColumns = FlussProjectionHelper.buildProjectedColumns(Set.of("sma5", "sma21"));

        var indicatorSource = FlussSource.<IndicatorRowFlexible>builder()
                .setBootstrapServers("coordinator-server:9123")
                .setDatabase("fluss")
                .setTable("indicators")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializationSchema(new IndicatorRowFlexibleDeserializationSchema(projectedColumns))
                .build();

        var marketDataSource = FlussSource.<CandleFlexible>builder()
                .setBootstrapServers("coordinator-server:9123")
                .setDatabase("fluss")
                .setTable("market_data_history")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializationSchema(new CandleFlexibleDeserializationSchema(List.of("time","symbol","close","volume")))
                .build();

        var rawIndicatorStream = env.fromSource(
                indicatorSource,
                WatermarkStrategy.<IndicatorRowFlexible>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                        .withTimestampAssigner((row, ts) -> row.timestamp),
                "Fluss Indicators Source"
        );

        var marketDataStream = env.fromSource(
                marketDataSource,
                WatermarkStrategy.<CandleFlexible>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                        .withTimestampAssigner((row, ts) -> row.timestamp),
                "Fluss Market Source"
        );

        // 5. Sink to Fluss
        var signalSink = FlussSink.<RowData>builder()
                .setBootstrapServers("coordinator-server:9123")
                .setDatabase("fluss")
                .setTable("strategy_signals")
                .setSerializationSchema(new RowDataSerializationSchema(true, false))
                .build();

        TradingStrategy strategy = new SimpleCrossoverStrategy("mycross");

        new GenericStrategyJob(strategy, rawIndicatorStream, marketDataStream, signalSink).run();

        env.execute("SMA Crossover Strategy");
    }
}
