package com.example.flink;

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.sink.serializer.RowDataSerializationSchema;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.example.flink.serializer.*;
import com.example.flink.strategy.TradingStrategy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GenericStrategyJob {

    public static void run(StreamExecutionEnvironment env, TradingStrategy strategy) throws Exception {

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

        // 2. Interval Join → IndicatorWithPrice
        var joinedStream = rawIndicatorStream
                .keyBy(ind -> ind.symbol)
                .intervalJoin(marketDataStream.keyBy(candle -> candle.symbol))
                .between(Time.milliseconds(-500), Time.milliseconds(500))
                .process(new ProcessJoinFunction<IndicatorRowFlexible, CandleFlexible, IndicatorWithPrice>() {
                    @Override
                    public void processElement(IndicatorRowFlexible indicator, CandleFlexible candle, Context ctx, Collector<IndicatorWithPrice> out) {
                        out.collect(new IndicatorWithPrice(
                                indicator.symbol,
                                indicator.timestamp,
                                candle.getClose(),
                                indicator.indicators.get("sma5"),
                                indicator.indicators.get("sma21"),
                                indicator.indicators.getOrDefault("rsi", 0.0)
                        ));
                    }
                });

        // 3. Detect Crossovers (ValueState)
        DataStream<StrategySignal> signals;
        if (strategy.isCEP()) {
            // CEP-based strategy
            signals = strategy.applyCEP(joinedStream);
        } else if (strategy.requiresState()) {
            // ValueState-based crossover strategy
            signals = joinedStream
                    .keyBy(ip -> ip.symbol)
                    .process(new ValueStateCrossoverProcessFunction(strategy));
        } else {
            // Simple per-event strategies (RSI, etc.)
            signals = joinedStream
                    .map(ip -> strategy.process(
                            new IndicatorRowFlexible(ip.symbol, ip.timestamp,
                                    Map.of("sma5", ip.sma5, "sma21", ip.sma21, "rsi", ip.rsi)),
                            new CandleFlexible(ip.symbol, ip.timestamp, Map.of("close", ip.close), 0L)
                    ))
                    .filter(s -> s != null);
        }

        // 4. Convert to RowData
        DataStream<RowData> signalRowData = signals.map(RowDataConverter::toRowData);

        // 5. Sink to Fluss
        var signalSink = FlussSink.<RowData>builder()
                .setBootstrapServers("coordinator-server:9123")
                .setDatabase("fluss")
                .setTable("strategy_signals")
                .setSerializationSchema(new RowDataSerializationSchema(true, false))
                .build();

        signalRowData.sinkTo(signalSink).name("Fluss Strategy Signals Sink");

        env.execute("Generic Strategy Job");
    }
}
