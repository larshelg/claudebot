package com.example.flink;

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.sink.serializer.RowDataSerializationSchema;
import com.alibaba.fluss.flink.source.FlinkSource;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.example.flink.serializer.*;
import com.example.flink.strategy.TradingStrategy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GenericStrategyJob implements Serializable {

    private final TradingStrategy strategy;
    private final DataStream<IndicatorRowFlexible> rawIndicatorStream;
    private final DataStream<CandleFlexible> marketDataStream;
    private final Sink<RowData> signalSink;

    public GenericStrategyJob(TradingStrategy strategy, DataStream<IndicatorRowFlexible> rawIndicatorStream,
            DataStream<CandleFlexible> marketDataStream, Sink<RowData> signalSink) {
        this.strategy = strategy;
        this.rawIndicatorStream = rawIndicatorStream;
        this.marketDataStream = marketDataStream;
        this.signalSink = signalSink;
    }

    public void run() throws Exception {

        // 2. Interval Join → IndicatorWithPrice
        var joinedStream = rawIndicatorStream
                .keyBy(ind -> ind.symbol)
                .intervalJoin(marketDataStream.keyBy(candle -> candle.symbol))
                .between(Time.milliseconds(-500), Time.milliseconds(500))
                .process(new ProcessJoinFunction<IndicatorRowFlexible, CandleFlexible, IndicatorWithPrice>() {
                    @Override
                    public void processElement(IndicatorRowFlexible indicator, CandleFlexible candle, Context ctx,
                            Collector<IndicatorWithPrice> out) {
                        out.collect(new IndicatorWithPrice(
                                indicator.symbol,
                                indicator.timestamp,
                                candle.getClose(),
                                indicator.indicators.get("sma5"),
                                indicator.indicators.get("sma21"),
                                indicator.indicators.getOrDefault("rsi", 0.0)));
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
                    .map(new StrategyProcessMapFunction(strategy))
                    .filter(s -> s != null);
        }

        // 4. Convert to RowData
        DataStream<RowData> signalRowData = signals.map(new RowDataMapFunction());

        signalRowData.sinkTo(signalSink).name("Fluss Strategy Signals Sink");

    }

    /**
     * Serializable MapFunction for processing strategy signals
     */
    private static class StrategyProcessMapFunction
            implements MapFunction<IndicatorWithPrice, StrategySignal>, Serializable {
        private final TradingStrategy strategy;

        public StrategyProcessMapFunction(TradingStrategy strategy) {
            this.strategy = strategy;
        }

        @Override
        public StrategySignal map(IndicatorWithPrice ip) throws Exception {
            return strategy.process(
                    new IndicatorRowFlexible(ip.symbol, ip.timestamp,
                            Map.of("sma5", ip.sma5, "sma21", ip.sma21, "rsi", ip.rsi)),
                    new CandleFlexible(ip.symbol, ip.timestamp, Map.of("close", ip.close), 0L));
        }
    }

    /**
     * Serializable MapFunction for converting StrategySignal to RowData
     */
    private static class RowDataMapFunction implements MapFunction<StrategySignal, RowData>, Serializable {
        @Override
        public RowData map(StrategySignal signal) throws Exception {
            return RowDataConverter.toRowData(signal);
        }
    }

}
