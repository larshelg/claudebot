package com.example.flink.jobs;

import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.sink.serializer.RowDataSerializationSchema;
import com.example.flink.domain.ExecReport;
import com.example.flink.domain.TradeSignal;
import com.example.flink.exchange.LocalTestOrderExecutionJob;
import com.example.flink.tradeengine.fluss.Adapters.TradeSignalDeserializationSchema;
import com.example.flink.tradeengine.fluss.Adapters.FlussRowMappers;
import com.example.flink.tradeengine.fluss.Adapters.PojoToFlussSink;
import com.example.flink.helpers.SerializableFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.time.Duration;
import java.util.function.Function;

/**
 * Cluster job that reads accepted trade signals from Fluss and emits
 * ExecReports via LocalTestOrderExecutionJob.
 */
public class LocalExecutionJob {
    public static void main(String[] args) throws Exception {
        String flussBootstrap = System.getProperty("fluss.bootstrap", "coordinator-server:9123");
        String flussDatabase = System.getProperty("fluss.database", "fluss");

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var tradeSignalSource = FlussSource.<TradeSignal>builder()
                .setBootstrapServers(flussBootstrap)
                .setDatabase(flussDatabase)
                .setTable("trade_signal_history")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializationSchema(new TradeSignalDeserializationSchema())
                .build();

        DataStream<TradeSignal> tradeSignals = env.fromSource(
                tradeSignalSource,
                WatermarkStrategy.<TradeSignal>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((t, ts) -> t.ts),
                "Fluss TradeSignal Source");

        // ExecReport sink to Fluss exec_report_history
        var execReportSink = FlussSink.<RowData>builder()
                .setBootstrapServers(flussBootstrap)
                .setDatabase(flussDatabase)
                .setTable("exec_report_history")
                .setSerializationSchema(new RowDataSerializationSchema(true, false))
                .build();

        // Process trade signals through local execution and sink to Fluss using
        // PojoToFlussSink
        LocalTestOrderExecutionJob.processTradeSignals(
                tradeSignals,
                new PojoToFlussSink<>(execReportSink, (SerializableFunction<ExecReport, RowData>) FlussRowMappers::execReportToRow));

        env.execute("Local Execution Job");
    }
}
