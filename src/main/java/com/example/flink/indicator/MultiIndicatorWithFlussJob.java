package com.example.flink.indicator;

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.sink.serializer.RowDataSerializationSchema;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.deserializer.RowDataDeserializationSchema;

import com.example.flink.Candle;
import com.example.flink.IndicatorRow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

/**
 * Multi-indicator processing job that writes results to Fluss.
 * 
 * This version of the job:
 * 1. Generates sample BTC price data
 * 2. Processes it through the multi-indicator pipeline
 * 3. Writes the results to a Fluss table for storage and further analysis
 */
public class MultiIndicatorWithFlussJob {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Keep it simple for local testing

        // Create Fluss source to read from market_data_history table
        FlussSource<RowData> flussSource = FlussSource.<RowData>builder()
                .setBootstrapServers("coordinator-server:9123")
                .setDatabase("fluss")
                .setTable("market_data_history")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializationSchema(new RowDataDeserializationSchema())
                .build();

        // Create DataStream from Fluss source
        DataStream<RowData> flussStream = env.fromSource(
                flussSource,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "Fluss Market Data Source");

        // Convert RowData to Candle objects
        DataStream<Candle> candleStream = flussStream.map(row -> {

            // Extract fields from RowData
            TimestampData timestampData = row.getTimestamp(0, 3); // timestamp field
            StringData symbolData = row.getString(1); // symbol field

            return new Candle(
                    symbolData.toString(), // symbol
                    timestampData.getMillisecond(), // timestamp in millis
                    row.getDecimal(2, 10, 4).toBigDecimal().doubleValue(), // open
                    row.getDecimal(3, 10, 4).toBigDecimal().doubleValue(), // high
                    row.getDecimal(4, 10, 4).toBigDecimal().doubleValue(), // low
                    row.getDecimal(5, 10, 4).toBigDecimal().doubleValue(), // close
                    row.getLong(6) // volume
            );
        });

        // Apply the multi-indicator process function
        DataStream<IndicatorRow> indicatorStream = candleStream
                .keyBy(candle -> candle.symbol)
                .process(new MultiIndicatorProcess());

        // Convert IndicatorRow to RowData for Fluss
        DataStream<RowData> rowDataStream = indicatorStream.map(indicator -> {
            GenericRowData row = new GenericRowData(9);
            row.setField(0, StringData.fromString(indicator.symbol));
            row.setField(1, TimestampData.fromEpochMillis(indicator.timestamp));
            row.setField(2, indicator.sma5);
            row.setField(3, indicator.sma14);
            row.setField(4, indicator.sma21);
            row.setField(5, indicator.ema5);
            row.setField(6, indicator.ema14);
            row.setField(7, indicator.ema21);
            row.setField(8, indicator.rsi14);
            return row;
        });

        // Print the results (for debugging)
        indicatorStream.print("Indicators");

        // Create Fluss sink using RowData
        FlussSink<RowData> flussSink = FlussSink.<RowData>builder()
                .setBootstrapServers("coordinator-server:9123") // Connect to Fluss in Docker
                .setDatabase("fluss")
                .setTable("indicators")
                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                .build();

        // Write to Fluss
        rowDataStream.sinkTo(flussSink).name("Fluss Indicators Sink");

        // Execute the job
        env.execute("Multi-Indicator Processing with Fluss Storage");
    }

}
