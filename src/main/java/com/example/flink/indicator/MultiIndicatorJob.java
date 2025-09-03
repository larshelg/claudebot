package com.example.flink.indicator;

import com.example.flink.domain.Candle;
import com.example.flink.IndicatorRow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * Simple main class to run the MultiIndicatorProcess locally for testing.
 *
 * This creates a sample stream of candle data and processes it through
 * the multi-indicator pipeline to compute SMA, EMA, and RSI values.
 */
public class MultiIndicatorJob {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Keep it simple for local testing

        // Create a sample data source
        DataStream<Candle> candleStream = env.addSource(new SampleCandleSource());

        // Apply the multi-indicator process function
        DataStream<IndicatorRow> indicatorStream = candleStream
                .keyBy(candle -> candle.symbol)
                .process(new MultiIndicatorProcess());

        // Print the results
        indicatorStream.print();

        // Execute the job
        env.execute("Multi-Indicator Processing Job");
    }

    /**
     * Sample source that generates realistic BTC candle data for testing
     */
    public static class SampleCandleSource implements SourceFunction<Candle> {
        private volatile boolean isRunning = true;
        private final Random random = new Random();
        private double currentPrice = 50000.0; // Starting BTC price

        @Override
        public void run(SourceContext<Candle> ctx) throws Exception {
            long timestamp = System.currentTimeMillis();

            while (isRunning) {
                // Generate realistic price movement (±2% max change)
                double changePercent = (random.nextDouble() - 0.5) * 0.04; // ±2%
                double newPrice = currentPrice * (1 + changePercent);

                // Create OHLC data with some realistic spread
                double open = currentPrice;
                double close = newPrice;
                double high = Math.max(open, close) * (1 + random.nextDouble() * 0.01); // Up to 1% higher
                double low = Math.min(open, close) * (1 - random.nextDouble() * 0.01); // Up to 1% lower
                double volume = 100 + random.nextDouble() * 1000; // Random volume 100-1100

                Candle candle = new Candle(
                        "BTCUSD",
                        timestamp,
                        open,
                        high,
                        low,
                        close,
                        volume);

                ctx.collect(candle);

                // Update current price and timestamp
                currentPrice = newPrice;
                timestamp += 60000; // Next minute

                // Sleep for demonstration (1 second = 1 minute of data)
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
