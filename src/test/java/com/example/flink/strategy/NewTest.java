package com.example.flink.strategy;

import com.example.flink.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class NewTest {

    @Test
   public void testCEPCrossoverProducesBuySignal() throws Exception {
        // 1. Create local Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Create test data
        List<IndicatorWithPrice> testData = List.of(
                new IndicatorWithPrice("BTCUSD", 1000L, 50000, 51000.0, 49000.0, 5.0),
                new IndicatorWithPrice("BTCUSD", 1001L, 51000, 52000.0, 50000.0, 22.0) // sma5 > sma21 -> BUY
        );

        // 3. Create source stream
        DataStreamSource<IndicatorWithPrice> source = env.fromCollection(testData);

        // 4. Apply CEP strategy
        CEPCrossoverStrategy strategy = new CEPCrossoverStrategy("testStrategy");
        DataStream<StrategySignal> signals = strategy.applyCEP(source);

        // 5. Collect results
        List<StrategySignal> result = signals.executeAndCollect(10);

        // 6. Assertions
        assertEquals(1, result.size());
        StrategySignal signal = result.get(0);
    }
}
