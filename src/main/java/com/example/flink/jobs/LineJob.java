package com.example.flink.jobs;

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.sink.serializer.RowDataSerializationSchema;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.example.flink.domain.Candle;
import com.example.flink.IndicatorRow;
import com.example.flink.domain.StrategySignal;
import com.example.flink.indicator.MultiIndicatorProcess;
import com.example.flink.indicators.SwingPointDetectorJob;
import com.example.flink.trend.TrendLineDetectorJob;
import com.example.flink.trend.TrendSummarizerFn;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.io.Serializable;
import java.time.Duration;

/**
 * Multi-indicator processing job that writes results to Fluss.
 *
 * This version of the job:
 * 1. Generates sample BTC price data
 * 2. Processes it through the multi-indicator pipeline
 * 3. Writes the results to a Fluss table for storage and further analysis
 */
public class LineJob {

        public static void main(String[] args) throws Exception {
                // Set up the execution environment
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1); // Keep it simple for local testing
                env.getConfig().disableObjectReuse(); // Disable object reuse to reduce memory pressure

                // Create Fluss source to read from market_data_history table
                var flussSource = FlussSource.<RowData>builder()
                                .setBootstrapServers("coordinator-server:9123")
                                .setDatabase("fluss")
                                .setTable("market_data_history")
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setDeserializationSchema(new RowDataDeserializationSchema())
                                .build();

                // Create DataStream from Fluss source with event time watermarks
                DataStream<RowData> flussStream = env.fromSource(flussSource,
                                WatermarkStrategy.<RowData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                                .withTimestampAssigner((row, timestamp) -> row.getTimestamp(0, 3)
                                                                .getMillisecond()),
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

                var barMs = 60_000L;
                // find swings
                var swings = SwingPointDetectorJob.build(
                                candleStream,
                                Duration.ofSeconds(5),
                                2, 2, // left/right
                                0.02 // minDeltaFrac
                );

                // Stage 2: trend lines with swings
                var lines = TrendLineDetectorJob.buildLinesWithSwings(
                                candleStream,
                                swings,
                                Duration.ofSeconds(5),
                                barMs,
                                24, 6, // window 24, slide 6
                                180, 256,
                                3.0, 0.12, // peakWeight, touchEpsFrac
                                1, 3, // breakoutPersistBars, falseBreakBars
                                3, 0.12, 0 // topK, minScore, minTouches
                );

                DataStream<TrendSummarizerFn.TrendSummary> summaries = lines
                                .keyBy(tl -> tl.symbol)
                                .process(new TrendSummarizerFn(
                                                /* lambdaSoftmax */ 3.0,
                                                /* betaSmoothing */ 0.45,
                                                /* fuseDelay */ Duration.ofMillis(10),
                                                /* recencyTau */ Duration.ofHours(1),
                                                /* enter BREAKOUT */ 0.25,
                                                /* exit BREAKOUT */ 0.15,
                                                /* trendAlpha */ 0.35,
                                                /* confThresh */ 0.25))
                                .name("trend-factor");

                // Convert IndicatorRow to RowData for Fluss
                DataStream<RowData> rowDataStream = summaries.map(sum -> {
                        GenericRowData row = new GenericRowData(11);
                        row.setField(0, StringData.fromString(sum.symbol));
                        row.setField(1, TimestampData.fromEpochMillis(sum.ts));
                        row.setField(2, sum.alpha);
                        row.setField(3, sum.confidence);
                        row.setField(4, StringData.fromString(sum.phase));
                        row.setField(5, StringData.fromString(sum.direction));
                        row.setField(6, sum.breakoutDirection != null ? StringData.fromString(sum.breakoutDirection)
                                        : null);
                        row.setField(7, TimestampData.fromEpochMillis(sum.lastBreakoutTs));
                        row.setField(8, sum.breakoutRecency);
                        row.setField(9, sum.breakoutStrength);
                        row.setField(10, sum.linesUsed);
                        return row;
                });

                // Create Fluss sink using RowData
                FlussSink<RowData> flussSink = FlussSink.<RowData>builder()
                                .setBootstrapServers("coordinator-server:9123") // Connect to Fluss in Docker
                                .setDatabase("fluss")
                                .setTable("trendsummary10")
                                .setSerializationSchema(new RowDataSerializationSchema(true, false))
                                .build();

                // Write to Fluss
                rowDataStream.sinkTo(flussSink).name("Fluss Indicators Sink");

                // Execute the job
                env.execute("LineJob");
        }

}
