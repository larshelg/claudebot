package com.example.flink;

import com.example.flink.strategy.SimpleCrossoverStrategy;
import com.example.flink.strategy.TradingStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CrossoverStrategyJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        TradingStrategy strategy = new SimpleCrossoverStrategy();
        GenericStrategyJob.run(env, strategy);

        env.execute("SMA Crossover Strategy");
    }
}
