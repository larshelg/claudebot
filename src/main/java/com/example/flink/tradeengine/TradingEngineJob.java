package com.example.flink.tradeengine;

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.sink.serializer.RowDataSerializationSchema;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.example.flink.domain.*;
import com.example.flink.tradeengine.fluss.Adapters.FlussRowMappers;
import com.example.flink.tradeengine.fluss.Adapters.PojoToFlussSink;
import com.example.flink.tradeengine.fluss.Adapters.ExecReportDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import java.time.Duration;
import java.util.Collections;

/**
 * TradingEngineJob wires PortfolioAndRiskJob to Fluss sources and sinks.
 *
 * Sources:
 * - Exec reports: fluss.exec_report_history (append)
 * - Trade signals: (optional) — default empty stream
 *
 * Sinks (configure via Fluss):
 * - Latest/upsert: open_positions_latest, portfolio_latest,
 * realized_pnl_latest, unrealized_pnl_latest
 * - History/append: exec_report_history, trade_match_history,
 * position_close_history, risk_alerts
 */
public class TradingEngineJob {

    public static void main(String[] args) throws Exception {
        String flussBootstrap = System.getProperty("fluss.bootstrap", "coordinator-server:9123");
        String flussDatabase = System.getProperty("fluss.database", "fluss");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1) Sources
        // ExecReports from Fluss (append-only history)
        var execReportSource = FlussSource.<ExecReport>builder()
                .setBootstrapServers(flussBootstrap)
                .setDatabase(flussDatabase)
                .setTable("exec_report_history")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializationSchema(new ExecReportDeserializationSchema())
                .build();

        DataStream<ExecReport> execReports = env.fromSource(
                execReportSource,
                WatermarkStrategy.<ExecReport>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((er, ts) -> er.ts),
                "Fluss ExecReport Source");

        // Optional trade signals source — default to empty
        DataStream<TradeSignal> tradeSignals = env.fromCollection(Collections.<TradeSignal>emptyList());

        // 2) Account policies (simple mapping from signals; can be replaced with real
        // source)
        DataStream<AccountPolicy> accountPolicies = tradeSignals
                .map(ts -> new AccountPolicy(ts.accountId, 3, "ACTIVE", 100_000.0, ts.ts))
                .returns(AccountPolicy.class);

        // 3) Build PortfolioAndRiskJob
        var job = new PortfolioAndRiskJob(tradeSignals, execReports, accountPolicies);

        // 4) Configure Fluss sinks
        // Build Fluss RowData sinks
        FlussSink<RowData> positionLatestSink = FlussSink.<RowData>builder()
                .setBootstrapServers(flussBootstrap)
                .setDatabase(flussDatabase)
                .setTable("open_positions_latest")
                .setSerializationSchema(new RowDataSerializationSchema(true, false))
                .build();

        FlussSink<RowData> portfolioLatestSink = FlussSink.<RowData>builder()
                .setBootstrapServers(flussBootstrap)
                .setDatabase(flussDatabase)
                .setTable("portfolio_latest")
                .setSerializationSchema(new RowDataSerializationSchema(true, false))
                .build();

        FlussSink<RowData> realizedLatestSink = FlussSink.<RowData>builder()
                .setBootstrapServers(flussBootstrap)
                .setDatabase(flussDatabase)
                .setTable("realized_pnl_latest")
                .setSerializationSchema(new RowDataSerializationSchema(true, false))
                .build();

        FlussSink<RowData> unrealizedLatestSink = FlussSink.<RowData>builder()
                .setBootstrapServers(flussBootstrap)
                .setDatabase(flussDatabase)
                .setTable("unrealized_pnl_latest")
                .setSerializationSchema(new RowDataSerializationSchema(true, false))
                .build();

        FlussSink<RowData> execReportHistorySink = FlussSink.<RowData>builder()
                .setBootstrapServers(flussBootstrap)
                .setDatabase(flussDatabase)
                .setTable("exec_report_history")
                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                .build();

        FlussSink<RowData> tradeMatchHistorySink = FlussSink.<RowData>builder()
                .setBootstrapServers(flussBootstrap)
                .setDatabase(flussDatabase)
                .setTable("trade_match_history")
                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                .build();

        FlussSink<RowData> positionCloseHistorySink = FlussSink.<RowData>builder()
                .setBootstrapServers(flussBootstrap)
                .setDatabase(flussDatabase)
                .setTable("position_close_history")
                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                .build();

        FlussSink<RowData> riskAlertsHistorySink = FlussSink.<RowData>builder()
                .setBootstrapServers(flussBootstrap)
                .setDatabase(flussDatabase)
                .setTable("risk_alerts")
                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                .build();

        // Wrap RowData sinks with POJO mappers
        job.setPositionSink(new PojoToFlussSink<>(positionLatestSink, FlussRowMappers::positionToRow));
        job.setPortfolioSink(new PojoToFlussSink<>(portfolioLatestSink, FlussRowMappers::portfolioToRow));
        job.setRealizedPnlSink(new PojoToFlussSink<>(realizedLatestSink, FlussRowMappers::realizedToRow));
        job.setUnrealizedPnlSink(new PojoToFlussSink<>(unrealizedLatestSink, FlussRowMappers::unrealizedToRow));

       // job.setExecReportSink(new PojoToFlussSink<>(execReportHistorySink, FlussRowMappers::execReportToRow));
        job.setTradeMatchSink(new PojoToFlussSink<>(tradeMatchHistorySink, FlussRowMappers::tradeMatchToRow));
        job.setPositionCloseSink(new PojoToFlussSink<>(positionCloseHistorySink, FlussRowMappers::positionCloseToRow));
        job.setRiskAlertSink(new PojoToFlussSink<>(riskAlertsHistorySink, FlussRowMappers::riskAlertToRow));

        // 5) Run
        job.run();
        env.execute("Trading Engine Job");
    }
}
